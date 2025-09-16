from pathlib import Path
import pandas as pd
from train_delays.parse import parse_changes_xml  # функция из src/train_delays/parse.py

def main():
    # Ищем последний файл изменений
    change_files = sorted(Path("data/raw").rglob("timetable_changes_*.xml"))
    if not change_files:
        raise FileNotFoundError("Нет файлов timetable_changes_*.xml в data/raw/. Сначала запусти fetch.")
    changes_file = change_files[-1]

    print(f"Читаем файл: {changes_file}")
    xml_text = Path(changes_file).read_text(encoding="utf-8")

    # Парсим XML → tidy DataFrame
    df = parse_changes_xml(xml_text)

    if df.empty:
        print("Парсер вернул пустой DataFrame. Сохраняю пустой CSV со схемой.")
        out_path = Path("data/processed/changes_parsed.csv")
        df.to_csv(out_path, index=False)
        print(f"Сохранено: {out_path}")
        return

    # Превью — самые полезные столбцы для обзора
    preview_cols = [
        "station", "eva", "stop_id", "scope", "event",
        "msg_type", "msg_code", "category", "priority",
        "ts", "from_ts", "to_ts", "duration_min",
    ]
    existing_preview = [c for c in preview_cols if c in df.columns]

    print("\nПервые 10 строк:")
    print(df[existing_preview].head(10).to_string(index=False))

    # Немного быстрой статистики
    print("\nЧастоты по типам сообщений (msg_type):")
    print(df["msg_type"].value_counts(dropna=False).head(10))

    if "category" in df.columns:
        print("\nТоп категорий (category):")
        print(df["category"].value_counts(dropna=False).head(10))

    # Сохранение CSV (как и для плана — в data/)
    out_path = Path("data/processed/changes_parsed.csv")
    df.to_csv(out_path, index=False)
    print(f"\nСохранено: {out_path}")

if __name__ == "__main__":
    main()
