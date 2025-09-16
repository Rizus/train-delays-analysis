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

## справочные таблицы для plan_parsed.csv и changes_parsed.csv

# plan_parsed.csv

| Колонка              | Описание                                         | Источник в XML (`plan`)                 |
| -------------------- | ------------------------------------------------ | --------------------------------------- |
| **station**          | Название станции                                 | `<timetable station="…">`               |
| **eva**              | EVA-ID станции (часто только в корне)            | `<timetable eva="…">` или `<s eva="…">` |
| **stop_id**          | Уникальный идентификатор слота/остановки         | `<s id="…">`                            |
| **event**            | Тип события: `ar` = прибытие, `dp` = отправление | тег `<ar>` или `<dp>`                   |
| **planned_ts**       | Плановое время (YYMMDDHHMM → datetime)           | атрибут `pt` в `<ar>/<dp>`              |
| **platform_planned** | Плановая платформа                               | `pp` в `<ar>/<dp>`                      |
| **platform_current** | Текущая платформа (редко есть в PLAN)            | `cp` в `<ar>/<dp>`                      |
| **line**             | Линия/маршрут (например `RE2`, `S7`)             | `l` в `<ar>/<dp>`                       |
| **path_pp**          | Плановый путь следования (через `                | `)                                      |
| **train_run_id**     | Идентификатор рейса/сцепки                       | `tra` в `<ar>/<dp>`                     |
| **wings**            | Признак сцепки (крылья)                          | `wings` в `<ar>/<dp>`                   |
| **tl_class**         | Класс продукта (`F`/`D`/`S`/`N` …)               | `f` в `<tl>`                            |
| **tl_type**          | Тип (`p` = плановый, …)                          | `t` в `<tl>`                            |
| **tl_operator**      | Оператор/код (например `"80"`, `"TDHS"`, `"R1"`) | `o` в `<tl>`                            |
| **tl_category**      | Категория поезда (`ICE`, `IC`, `RE`, `S` …)      | `c` в `<tl>`                            |
| **tl_number**        | Номер поезда                                     | `n` в `<tl>`                            |

# changes_parsed.csv

| Колонка      | Описание                                                                | Источник в XML (`changes`)              |
| ------------ | ----------------------------------------------------------------------- | --------------------------------------- |
| **station**  | Название станции                                                        | `<timetable station="…">`               |
| **eva**      | EVA-ID станции                                                          | `<timetable eva="…">` или `<s eva="…">` |
| **stop_id**  | Уникальный идентификатор слота                                          | `<s id="…">`                            |
| **scope**    | Где найдено сообщение: `s` (общая), `ar` (прибытие), `dp` (отправление) | уровень в XML                           |
| **event**    | Тип события (`ar`/`dp`), если scope соответствует                       | тег `<ar>` или `<dp>`                   |
| **event_ct** | Фактическое время события (корректировка)                               | атрибут `ct` в `<ar>/<dp>`              |
| **platform** | Актуальная платформа                                                    | `cp` в `<ar>/<dp>`                      |
| **line**     | Линия/маршрут (может измениться)                                        | `l` в `<ar>/<dp>`                       |
| **path**     | Актуальный маршрут (через `                                             | `)                                      |
| **msg_id**   | Уникальный ID сообщения                                                 | `<m id="…">`                            |
| **msg_type** | Тип сообщения: `d` (delay), `f` (fallback), `h` (Hinweis/инфо)          | `t` в `<m>`                             |
| **msg_code** | Код события/задержки (например `43`, `45`, `13`)                        | `c` в `<m>`                             |
| **category** | Категория текста (например `"Information"`, `"Störung …"`)              | `cat` в `<m>`                           |
| **priority** | Приоритет сообщения (1 = высокий, 2/3 = ниже)                           | `pr` в `<m>`                            |
| **ts**       | Timestamp сообщения (YYMMDDHHMM → datetime)                             | `ts` в `<m>`                            |
| **from_ts**  | Время начала действия (если ограничено)                                 | `from` в `<m>`                          |
| **to_ts**    | Время окончания действия (если ограничено)                              | `to` в `<m>`                            |
| **ts_tts**   | Точная текстовая форма времени (с миллисекундами)                       | `ts-tts` в `<m>`                        |