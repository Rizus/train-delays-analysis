# scripts/parse_plan.py
from pathlib import Path
import pandas as pd
from train_delays.parse import parse_timetable_xml

# Находим последний выгруженный план (по имени файла)
plan_files = sorted(Path("data/raw").rglob("timetable_plan_*.xml"))
if not plan_files:
    raise FileNotFoundError("Нет файлов timetable_plan_*.xml в data/raw/")
plan_file = plan_files[-1]

print(f"Читаем файл: {plan_file}")
xml_text = Path(plan_file).read_text(encoding="utf-8")

# Парсим XML в DataFrame
df = parse_timetable_xml(xml_text)

# Быстрая витрина для консоли
preview_cols = [
    "station", "event", "planned_ts", "platform_planned",
    "line", "tl_category", "tl_number", "tl_operator"
]
print("\nПервые 10 строк:")
print(df[preview_cols].head(10).to_string(index=False))

# Статистика
print(f"\nВсего строк: {len(df)}")
print("По типам событий:\n", df["event"].value_counts(dropna=False))

# Сохраняем результат в CSV
out_path = Path("data/processed/plan_parsed.csv")
df.to_csv(out_path, index=False)
print(f"\nСохранено: {out_path}")
