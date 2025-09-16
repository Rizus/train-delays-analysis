# Train Delays Analysis (Deutsche Bahn Open Data)

url: https://developers.deutschebahn.com/db-api-marketplace/apis/

Аналитический проект для работы с **Deutsche Bahn Timetables API**: загрузка расписаний, отслеживание изменений и анализ задержек поездов.

## Структура проекта
```markdown
.
├── data/ # Данные
│ ├── raw/ # Сырые XML-выгрузки из API
│ ├── processed/ # Парсинг в tidy-таблицы CSV
│ └── README.md # Краткое описание источников данных
├── notebooks/ # Jupyter ноутбуки для анализа
│ └── 01_delays_eda.ipynb
├── reports/ # Отчёты, графики
│ └── figures/
├── scripts/ # Утилитарные скрипты
│ ├── parse_plan.py
│ ├── parse_changes.py
│ ├── merge_plan_changes.py
│ └── init.py
├── src/train_delays/ # Логика работы с API и парсинг XML
│ ├── fetch.py
│ ├── parse.py
│ └── features.py (WIP)
├── requirements.txt # Зависимости
├── pyproject.toml # Метаданные проекта
├── LICENSE
└── README.md # Этот файл
```

## Установка и подготовка

1. Клонировать репозиторий и перейти в него:
   ```bash
	git clone https://github.com/<yourname>/train-delays-analysis.git
   	cd train-delays-analysis
    ```

2. Создать виртуальное окружение (пример через venv):
   ```bash
   python3 -m venv db_venv
   source db_venv/bin/activate
   ```

3. Установить зависимости:
   ```bash
   pip install -r requirements.txt
   ```
   
4. Создать файл .env в корне проекта:
	```markdown
 	# DB API Marketplace credentials (https://apis.deutschebahn.com/db-api-marketplace/)
	DB_CLIENT_ID=...
	DB_API_KEY=...

	# Default station and time (optional)
	DEFAULT_STATION=Hannover Hbf
	DEFAULT_EVA =8000152
	DEFAULT_DATE=20250916      # YYYY-MM-DD
	DEFAULT_HOUR=8             # 0-23
 	```

## Получение данных

1. Запуск основной загрузки:
   ```bash
	python -m train_delays.fetch
   ```

	Файлы сохраняются в:
	```markdown
	data/raw/YYYYMMDD_HHMM/
	  ├─ stations_<NAME>.json
	  ├─ timetable_plan_<EVA>_<STAMP>.xml
	  └─ timetable_changes_<EVA>_<STAMP>.xml
    ```


2. Парсинг данных
	- План:
		```bash
		python -m scripts.parse_plan
		# → data/processed/plan_parsed.csv
  		```

   - Изменения:
     	```bash
		python -m scripts.parse_changes
		# → data/processed/changes_parsed.csv
        ```

   - В работе! -> Мердж и расчёт задержек:
    	```bash
		python -m scripts.merge_plan_changes --tolerance-min 3
		# → data/processed/merged_with_delays.csv
    	```
      
## Анализ
Открыть ноутбук:
```markdown
notebooks/01_delays_eda.ipynb
```
В нём первые графики: распределение задержек, топ-станции по проблемам, частота по категориям.


## План спринтов

Sprint 1: Подключение к DB API, выгрузка «живых» окон (done)

Sprint 2: Парсинг, merge, подсчёт задержек, базовый EDA (in process)

Sprint 3: Построение словаря кодов причин, дашборды (Plotly, Streamlit)

Sprint 4: Автоматизация сбора (несколько станций, дни/часы), база данных (Postgres)