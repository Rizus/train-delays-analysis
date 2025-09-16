from __future__ import annotations
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, List, Dict, Any
from zoneinfo import ZoneInfo
import pandas as pd

# =============== helpers ===============

def _parse_ts_yyMMddHHmm(s: Optional[str], tz: str = "Europe/Berlin") -> Optional[datetime]:
    """
    Унифицированный парсер таймштампа YYMMDDHHMM → TZ-aware datetime.
    Возвращает None для пустых/битых значений.
    """
    if not s or len(s) != 10 or not s.isdigit():
        return None
    yy = int(s[0:2]); year = 2000 + yy
    month = int(s[2:4]); day = int(s[4:6])
    hour = int(s[6:8]); minute = int(s[8:10])
    return datetime(year, month, day, hour, minute, tzinfo=ZoneInfo(tz))


def _first_or_none(node: ET.Element, tag: str) -> Optional[ET.Element]:
    """Возвращает первый прямой дочерний узел <tag> или None (НЕ рекурсивно)."""
    return next(iter(node.findall(tag)), None)


# =============== PLAN parser ===============

def parse_timetable_xml(xml_text: str, tz: str = "Europe/Berlin") -> pd.DataFrame:
    """
    Разбирает PLAN-XML (<timetable> ... <s> ... <ar/>, <dp/>, <tl/> ... ) в tidy-таблицу.

    Возвращает столбцы:
      - station, eva, stop_id
      - event ('ar'|'dp')
      - planned_ts (dt[tz])
      - platform_planned (= @pp), platform_current (= @cp, если вдруг есть)
      - line (@l), path_pp (@ppth), train_run_id (@tra), wings (@wings)
      - tl_*: метаданные поезда из <tl …> при данном <s> (берём первый <tl>)
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        snippet = (xml_text or "")[:300].replace("\n", " ")
        raise RuntimeError(f"Invalid plan XML: {e}. Snippet: {snippet}")

    station_name = root.attrib.get("station")
    eva_from_root = root.attrib.get("eva")  # иногда eva только в корне
    rows: List[Dict[str, Any]] = []

    for s in root.findall(".//s"):
        stop_id = s.attrib.get("id")
        eva = s.attrib.get("eva") or eva_from_root  # ← fallback к корню

        # метаданные поезда из <tl …> (часто один; берём первый)
        tl = _first_or_none(s, "tl")
        tl_class    = tl.attrib.get("f") if tl is not None else None
        tl_type     = tl.attrib.get("t") if tl is not None else None
        tl_operator = tl.attrib.get("o") if tl is not None else None
        tl_category = tl.attrib.get("c") if tl is not None else None
        tl_number   = tl.attrib.get("n") if tl is not None else None

        # поддерживаем несколько <ar>/<dp> в одном <s>
        for tag in ("ar", "dp"):
            for node in s.findall(tag):
                pt = _parse_ts_yyMMddHHmm(node.attrib.get("pt"), tz=tz)

                rows.append({
                    "station": station_name,
                    "eva": eva,
                    "stop_id": stop_id,
                    "event": tag,                                # 'ar' | 'dp'
                    "planned_ts": pt,
                    "platform_planned": node.attrib.get("pp"),
                    "platform_current": node.attrib.get("cp"),
                    "line": node.attrib.get("l"),
                    "path_pp": node.attrib.get("ppth"),
                    "train_run_id": node.attrib.get("tra"),
                    "wings": node.attrib.get("wings"),
                    "tl_class": tl_class,
                    "tl_type": tl_type,
                    "tl_operator": tl_operator,
                    "tl_category": tl_category,
                    "tl_number": tl_number,
                })

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "station","eva","stop_id","event","planned_ts",
                "platform_planned","platform_current","line",
                "path_pp","train_run_id","wings",
                "tl_class","tl_type","tl_operator","tl_category","tl_number",
            ]
        )

    df = df.sort_values("planned_ts", na_position="last").reset_index(drop=True)
    for col in ["station","eva","stop_id","event","platform_planned","platform_current",
                "line","path_pp","train_run_id","wings","tl_class","tl_type","tl_operator",
                "tl_category","tl_number"]:
        df[col] = df[col].astype("string")
    return df


# =============== CHANGES parser ===============

def parse_changes_xml(xml_text: str, tz: Optional[str] = "Europe/Berlin") -> pd.DataFrame:
    """
    Парсит XML из /timetables/v1/fchg/{eva}.
    Каждое сообщение <m ...> становится строкой с контекстом, где оно найдено.

    Столбцы:
      - station, eva, stop_id
      - scope   : 's' | 'ar' | 'dp'
      - event   : 'ar'/'dp' если scope соответствует, иначе <NA>
      - event_ct: фактическое время события (ct) из <ar>/<dp> (datetime)
      - platform: cp из <ar>/<dp>, line: l, path: cpth
      - msg_id, msg_type (t), msg_code (c), category (cat), priority (pr)
      - ts, from_ts, to_ts (datetime), ts_tts (строка из XML)
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        snippet = (xml_text or "")[:300].replace("\n", " ")
        raise RuntimeError(f"Invalid fchg XML: {e}. Snippet: {snippet}")

    station = root.attrib.get("station")
    eva_root = root.attrib.get("eva")

    rows: List[Dict[str, Any]] = []

    def _append_row(m: ET.Element, *, scope: str, event: Optional[str], s_node: ET.Element, ardp_node: Optional[ET.Element]):
        # контекст уровня <s>
        stop_id = s_node.attrib.get("id")
        eva = s_node.attrib.get("eva") or eva_root

        # контекст уровня <ar>/<dp>
        ct   = _parse_ts_yyMMddHHmm(ardp_node.attrib.get("ct"), tz=tz) if ardp_node is not None else None
        cp   = ardp_node.attrib.get("cp")  if ardp_node is not None else None
        line = ardp_node.attrib.get("l")   if ardp_node is not None else None
        cpth = ardp_node.attrib.get("cpth") if ardp_node is not None else None

        # атрибуты сообщения
        attrs   = m.attrib
        ts      = _parse_ts_yyMMddHHmm(attrs.get("ts"), tz=tz)
        from_ts = _parse_ts_yyMMddHHmm(attrs.get("from"), tz=tz)
        to_ts   = _parse_ts_yyMMddHHmm(attrs.get("to"), tz=tz)

        rows.append({
            "station": station,
            "eva": eva,
            "stop_id": stop_id,
            "scope": scope,
            "event": event,
            "event_ct": ct,
            "platform": cp,
            "line": line,
            "path": cpth,
            "msg_id": attrs.get("id"),
            "msg_type": attrs.get("t"),
            "msg_code": attrs.get("c"),
            "category": attrs.get("cat"),
            "priority": attrs.get("pr"),
            "ts": ts,
            "from_ts": from_ts,
            "to_ts": to_ts,
            "ts_tts": attrs.get("ts-tts"),
        })

    # сообщения прямо под <s>
    for s in root.findall(".//s"):
        for m in s.findall("m"):
            _append_row(m, scope="s", event=None, s_node=s, ardp_node=None)
        # сообщения внутри <ar>
        for ar in s.findall("ar"):
            for m in ar.findall("m"):
                _append_row(m, scope="ar", event="ar", s_node=s, ardp_node=ar)
        # сообщения внутри <dp>
        for dp in s.findall("dp"):
            for m in dp.findall("m"):
                _append_row(m, scope="dp", event="dp", s_node=s, ardp_node=dp)

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "station","eva","stop_id","scope","event","event_ct","platform","line","path",
                "msg_id","msg_type","msg_code","category","priority","ts","from_ts","to_ts","ts_tts"
            ]
        )

    df = df.sort_values(["ts", "event_ct"], na_position="last").reset_index(drop=True)
    for col in ["station","eva","stop_id","scope","event","platform","line","path",
                "msg_id","msg_type","msg_code","category","priority","ts_tts"]:
        df[col] = df[col].astype("string")
    return df
