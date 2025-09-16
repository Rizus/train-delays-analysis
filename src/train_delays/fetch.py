from __future__ import annotations

import os, time, json, pathlib
from typing import Any, Dict, List, Optional, Union
from zoneinfo import ZoneInfo
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter, Retry
from dotenv import load_dotenv
import xml.etree.ElementTree as ET


# --- Константы/настройки ---
BERLIN = ZoneInfo("Europe/Berlin")                          # локальная TZ (для меток времени и дефолтов)
BASE = "https://apis.deutschebahn.com/db-api-marketplace/apis"
USER_AGENT = "train-delays-analysis/0.1 (+github.com/yourname)"
HTTP_TIMEOUT_S = 30
RETRY_TOTAL = 5
RETRY_BACKOFF = 0.5
RETRY_STATUSES = (429, 500, 502, 503, 504)
RAW_SAVE_PAUSE_S = 0.2

load_dotenv()  # подтягиваем DB_CLIENT_ID и DB_API_KEY из .env


# ----------------- утилиты -----------------
def _make_session() -> requests.Session:
    """HTTP-сессия с ретраями и UA."""
    s = requests.Session()
    retries = Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=RETRY_STATUSES,
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": USER_AGENT})
    return s


SESSION = _make_session()


def _headers() -> Dict[str, str]:
    """Общие заголовки + ключи из окружения."""
    client_id = os.getenv("DB_CLIENT_ID")
    api_key = os.getenv("DB_API_KEY")
    if not client_id or not api_key:
        raise RuntimeError("Missing DB_CLIENT_ID or DB_API_KEY in environment (.env).")
    return {
        "Accept": "application/json",  # для XML поменяем ниже
        "DB-Client-Id": client_id,
        "DB-Api-Key": api_key,
        "User-Agent": USER_AGENT,
    }


def _ensure_dir(path: Union[str, pathlib.Path]) -> pathlib.Path:
    p = pathlib.Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _save_text(content: str, path: Union[str, pathlib.Path]) -> None:
    path = pathlib.Path(path)
    _ensure_dir(path.parent)
    path.write_text(content, encoding="utf-8")


def _save_json(obj: Any, path: Union[str, pathlib.Path]) -> None:
    path = pathlib.Path(path)
    _ensure_dir(path.parent)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_stations_xml(xml_text: str) -> List[Dict[str, Any]]:
    """Парсинг ответа /timetables/v1/station/{name} → список станций с evaNr."""
    root = ET.fromstring(xml_text)
    out: List[Dict[str, Any]] = []
    for st in root.findall(".//station"):
        out.append({
            "name": st.attrib.get("name"),
            "evaNr": st.attrib.get("eva") or st.attrib.get("evaNr"),
            "ds100": st.attrib.get("ds100"),
            "db": st.attrib.get("db"),
            "creationts": st.attrib.get("creationts"),
        })
    return out


def _to_yymmdd(date_str: Optional[str], when: Optional[datetime] = None) -> str:
    """
    Приводит дату к формату YYMMDD (как требует /plan/{eva}/{YYMMDD}/{HH}).
    Принимает: "YYMMDD" | "YYYYMMDD" | "YYYY-MM-DD" | None.
    Если None — берём дату из `when` (или текущее локальное время).
    """
    if date_str:
        s = date_str.strip()
        if "-" in s:  # "YYYY-MM-DD"
            y, m, d = s.split("-")
            return f"{int(y) % 100:02d}{int(m):02d}{int(d):02d}"
        if len(s) == 8 and s.isdigit():  # "YYYYMMDD"
            return s[2:]
        if len(s) == 6 and s.isdigit():  # "YYMMDD"
            return s
        raise ValueError("date must be YYMMDD | YYYYMMDD | YYYY-MM-DD")
    when = when or datetime.now(BERLIN)
    return when.strftime("%y%m%d")


# ----------------- публичный API -----------------
def find_station(name: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Поиск станции через timetables XML-эндпоинт.
    В ответе важно поле eva/evaNr (нужно для plan/fchg).
    """
    headers = _headers()
    url_xml = f"{BASE}/timetables/v1/station/{requests.utils.quote(name)}"
    r = SESSION.get(url_xml, headers=headers, timeout=HTTP_TIMEOUT_S)
    r.raise_for_status()
    ct = (r.headers.get("Content-Type") or "").lower()
    if "xml" in ct:
        return _parse_stations_xml(r.text)[:limit]
    snippet = (r.text or "")[:300].replace("\n", " ")
    raise RuntimeError(f"Station search failed: {r.status_code} {ct}. Body: {snippet}")


def get_planned_timetable(eva: Union[int, str], date: str, hour: int) -> str:
    """
    План по станции за конкретный час (официальная форма):
      /timetables/v1/plan/{eva}/{YYMMDD}/{HH}
    """
    url = f"{BASE}/timetables/v1/plan/{eva}/{date}/{int(hour):02d}"  # HH всегда две цифры
    headers = _headers()
    headers["Accept"] = "application/xml"  # план возвращается в XML
    r = SESSION.get(url, headers=headers, timeout=HTTP_TIMEOUT_S)
    r.raise_for_status()
    return r.text


def get_changes(eva: Union[int, str]) -> str:
    """Изменения (XML) по станции: /timetables/v1/fchg/{eva} (без даты/часа)."""
    headers = _headers()
    headers["Accept"] = "application/xml"
    url = f"{BASE}/timetables/v1/fchg/{eva}"
    r = SESSION.get(url, headers=headers, timeout=HTTP_TIMEOUT_S)
    r.raise_for_status()
    return r.text


def fetch_and_save_raw(
    station_name: str,
    outdir: Union[str, pathlib.Path] = "data/raw",
    date: Optional[str] = None,   # допускаем гибкие форматы; приведём к YYMMDD
    hour: Optional[int] = None    # 0..23; если None — возьмём текущий локальный час
) -> Dict[str, pathlib.Path]:
    """
    1) Находим станцию → берём eva.
    2) Грузим PLAN строго по форме {YYMMDD}/{HH}.
    3) Грузим FCHG (без даты/часа).
    4) Сохраняем в data/raw/<YYYYMMDD_HHMM>/.
    """
    stations = find_station(station_name, limit=1)
    if not stations:
        raise ValueError(f"No station found for name='{station_name}'")
    eva = stations[0].get("evaNr") or stations[0].get("evaNo") or stations[0].get("number")
    if not eva:
        raise ValueError("EVA number not found in station payload.")

    # Локальные «сейчас» (для метки папки и дефолтов даты/часа)
    now_local = datetime.now(BERLIN)

    # Приводим входную дату к YYMMDD (или берём текущую)
    yymmdd = _to_yymmdd(date, when=now_local)
    # Выбираем час: если не передали — используем текущий
    hh = now_local.hour if hour is None else int(hour)

    # Метка времени папки (для удобства)
    stamp = now_local.strftime("%Y%m%d_%H%M")
    base = _ensure_dir(pathlib.Path(outdir) / stamp)

    # Сохраняем метаданные поиска станции (для воспроизводимости)
    stations_path = base / f"stations_{station_name}.json"
    _save_json(stations, stations_path)

    # PLAN: официальный эндпоинт с YYMMDD/HH
    plan_xml = get_planned_timetable(eva=eva, date=yymmdd, hour=hh)
    plan_path = base / f"timetable_plan_{eva}_{stamp}.xml"
    _save_text(plan_xml, plan_path)

    # Небольшая пауза — бережём лимиты
    time.sleep(RAW_SAVE_PAUSE_S)

    # FCHG: текущее окно изменений (без даты/часа)
    changes_xml = get_changes(eva=eva)
    changes_path = base / f"timetable_changes_{eva}_{stamp}.xml"
    _save_text(changes_xml, changes_path)

    return {"stations": stations_path, "plan_xml": plan_path, "changes_xml": changes_path}


if __name__ == "__main__":
    # Можно задать DEFAULT_STATION/DEFAULT_DATE/DEFAULT_HOUR в .env
    station = os.getenv("DEFAULT_STATION")
    date_env = os.getenv("DEFAULT_DATE")   # "YYMMDD" | "YYYYMMDD" | "YYYY-MM-DD" | пусто
    hour_env = os.getenv("DEFAULT_HOUR")   # "0..23" | пусто
    hour_val = None if not hour_env else int(hour_env)

    paths = fetch_and_save_raw(station_name=station, date=date_env, hour=hour_val)
    print("Saved:", {k: str(v) for k, v in paths.items()})
