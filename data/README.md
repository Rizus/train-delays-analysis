# Data: Deutsche Bahn Open Data (Timetables)

## Источник
- DB API Marketplace — products: Timetables (и fchg для изменений)

## Как получить сырьё
1) В .env добавить:
   # DB API Marketplace credentials (https://apis.deutschebahn.com/db-api-marketplace/)
   DB_CLIENT_ID=...
   DB_API_KEY=...

   # Default station and time (optional)
   DEFAULT_STATION=Hannover Hbf
   DEFAULT_EVA =8000152
   DEFAULT_DATE=20250916      # YYYY-MM-DD
   DEFAULT_HOUR=8             # 0-23

2) Запуск:
   python -m train_delays.fetch

## Что появляется
data/raw/YYYYMMDD_HHMM/
  ├─ stations_<NAME>.json
  ├─ timetable_plan_<EVA>_<STAMP>.xml    # plan/{eva}/{YYMMDD}/{HH}
  └─ timetable_changes_<EVA>_<STAMP>.xml # fchg/{eva}

## Парсинг
python -m scripts.parse_plan     # → data/processed/plan_parsed.csv
python -m scripts.parse_changes  # → data/processed/changes_parsed.csv
-> (В работе...) python -m scripts.merge_plan_changes --tolerance-min 3
                                 # → data/merged_with_delays.csv (с delay_min)
