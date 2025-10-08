# scripts/merge_plan_changes.py
# Запуск из корня репо:
#   python -m scripts.merge_plan_changes --tolerance-min 2
#
# Требует:
#   data/processed/plan_parsed.csv     (из scripts/parse_plan.py)
#   data/processed/changes_parsed.csv  (из scripts/parse_changes.py)

from pathlib import Path
import argparse
import pandas as pd
import numpy as np

PLAN_CSV = Path("data/processed/plan_parsed.csv")
CHG_CSV  = Path("data/processed/changes_parsed.csv")
OUT_CSV  = Path("data/processed/merged_with_delays.csv")


def _read_plan() -> pd.DataFrame:
    if not PLAN_CSV.exists():
        raise FileNotFoundError(f"Нет файла {PLAN_CSV}. Сначала запусти scripts/parse_plan.py")
    # planned_ts — tz-aware ISO (из парсера), пусть pandas сам распознает
    df = pd.read_csv(
        PLAN_CSV,
        parse_dates=["planned_ts"],
        dtype="string",
        keep_default_na=True,
        na_values=["", "NA", "NaN", "null"],
    )

    df["planned_ts"] = pd.to_datetime(df["planned_ts"], errors="coerce", utc=True).dt.tz_convert("Europe/Berlin")

    # Явно приводим некоторые текстовые поля к StringDtype (чтобы не было object)
    for col in ["station","eva","stop_id","event","platform_planned","platform_current",
                "line","path_pp","train_run_id","wings","tl_class","tl_type",
                "tl_operator","tl_category","tl_number"]:
        if col in df.columns:
            df[col] = df[col].astype("string")
    # Сортировка обязательна для merge_asof
    return df.sort_values("planned_ts").reset_index(drop=True)


def _read_changes() -> pd.DataFrame:
    if not CHG_CSV.exists():
        raise FileNotFoundError(f"Нет файла {CHG_CSV}. Сначала запусти scripts/parse_changes.py")

    # читаем всё как строки, а потом вручную конвертируем даты
    df = pd.read_csv(
        CHG_CSV,
        dtype="string",
        keep_default_na=True,
        na_values=["", "NA", "NaN", "null"],
    )

    # Конвертируем колонки с временем вручную
    for col in ["ts", "from_ts", "to_ts", "event_ct"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True).dt.tz_convert("Europe/Berlin")

    # Приведём все текстовые поля к StringDtype
    for col in ["station","eva","stop_id","scope","event","platform","line","path",
                "msg_id","msg_type","msg_code","category","priority","ts_tts"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Определим фактическое время изменения
    df["change_time"] = df["event_ct"].where(df["event_ct"].notna(), df["ts"])

    # Обязательно сортировка по времени
    return df.sort_values("change_time").reset_index(drop=True)


def _merge_one_event(df_plan: pd.DataFrame, df_chg: pd.DataFrame, event: str, tol: pd.Timedelta) -> pd.DataFrame:
    """Мерджим отдельно для 'ar' или 'dp', чтобы не путать типы событий."""
    left = df_plan[df_plan["event"] == event].copy()
    right = df_chg[df_chg["event"] == event].copy()

    # Если есть stop_id — используем его как строгий ключ (by=["stop_id"])
    # Это минимизирует ложные матчи между разными поездами.
    # Если в твоих данных stop_id иногда пустой, можно fallback'нуть на merge без 'by' (см. ниже).
    have_ids_left  = left["stop_id"].notna().any()  if "stop_id" in left.columns  else False
    have_ids_right = right["stop_id"].notna().any() if "stop_id" in right.columns else False
    use_by = have_ids_left and have_ids_right

    # Подготавливаем правые фичи, чтобы не схлопнуть имена при merge
    suffix = "_chg"
    right_renamed = right.rename(columns={
        "platform": f"platform{suffix}",
        "line":     f"line{suffix}",
        "path":     f"path{suffix}",
        "msg_type": f"msg_type{suffix}",
        "msg_code": f"msg_code{suffix}",
        "category": f"category{suffix}",
        "priority": f"priority{suffix}",
    })

    # merge_asof: ближайшее изменение к planned_ts в пределах допусука
    merged = pd.merge_asof(
        left.sort_values("planned_ts"),
        right_renamed.sort_values("change_time"),
        left_on="planned_ts",
        right_on="change_time",
        by=["stop_id"] if use_by else None,   # если stop_id не задан — мерджим без группировки
        direction="nearest",
        tolerance=tol,
    )

    # Вычисляем фактическое время события (последняя версия после мерджа)
    # Приоритет: event_ct (если было) → change_time (обычно = event_ct/ts) → оставим NaT
    if "event_ct" in merged.columns:
        merged["changed_ts"] = merged["event_ct"].where(merged["event_ct"].notna(), merged["change_time"])
    else:
        merged["changed_ts"] = merged["change_time"]

    # Считаем задержку в минутах (Int64 с NaN как <NA>)
    merged["delay_min"] = (
        (merged["changed_ts"] - merged["planned_ts"])
        .dt.total_seconds()
        .rdiv(60)  #  seconds / 60  (аналог //, но вернёт float)
        .round()
        .astype("Int64")
    )

    # Актуальная платформа: если из changes пришла platform_chg — используем её,
    # иначе оставляем platform_planned
    merged["platform_actual"] = merged.get("platform_chg", pd.Series([pd.NA]*len(merged)))
    merged["platform_actual"] = merged["platform_actual"].where(merged["platform_actual"].notna(),
                                                                merged.get("platform_planned"))

    return merged


def main():
    parser = argparse.ArgumentParser(description="Merge plan & changes с расчётом delay_min")
    parser.add_argument("--tolerance-min", type=int, default=2,
                        help="Допуск по времени для match plan↔changes в минутах (по умолчанию 2)")
    parser.add_argument("--out", type=str, default=str(OUT_CSV),
                        help=f"Путь для сохранения CSV с мерджем (по умолчанию {OUT_CSV})")
    args = parser.parse_args()

    tol = pd.Timedelta(minutes=int(args.tolerance_min))

    print(f"Читаем план:    {PLAN_CSV}")
    df_plan = _read_plan()
    print(f"Читаем изменения:{CHG_CSV}")
    df_chg = _read_changes()

    # Мерджим по типам событий отдельно
    print(f"Мерджим с допуском ±{args.tolerance_min} мин...")
    parts = []
    for ev in ("ar", "dp"):
        part = _merge_one_event(df_plan, df_chg, event=ev, tol=tol)
        parts.append(part)

    merged = pd.concat(parts, ignore_index=True)

    # Немного «витринных» столбцов (оставим и все остальные на всякий случай)
    view_cols = [
        "station","eva","stop_id","event",
        "planned_ts","changed_ts","delay_min",
        "platform_planned","platform_actual",
        "line","tl_category","tl_number","tl_operator",
        "msg_type_chg","msg_code_chg","category_chg","priority_chg",
    ]
    existing = [c for c in view_cols if c in merged.columns]
    print("\nПревью (10 строк):")
    print(merged[existing].head(10).to_string(index=False))

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)
    print(f"\nСохранено: {out_path}")
    print("Кол-во строк:", len(merged))
    if "delay_min" in merged.columns:
        print("Доля строк с задержкой > 0 мин:",
              float((merged['delay_min'].fillna(0) > 0).mean()) if len(merged) else 0.0)


if __name__ == "__main__":
    main()
