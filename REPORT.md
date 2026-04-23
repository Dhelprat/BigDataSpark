# Отчет по лабораторной работе №2

**Дисциплина:** Анализ Больших Данных  
**Тема:** Реализация ETL-пайплайна на Apache Spark и построение витрин в ClickHouse  
**Группа:** М8О-315Б-23  
**Выполнила:** Лапенко Карина Александровна

## 1. Цель работы
Реализовать ETL-процесс на Apache Spark, который:
1. Загружает и преобразует исходные данные в модель «звезда» в PostgreSQL.
2. Строит аналитические витрины на основе модели «звезда» и загружает их в ClickHouse.

## 2. Исходные данные
- Количество файлов: **10** (`MOCK_DATA*.csv`)
- Количество записей в каждом файле: **1000**
- Общий объем данных: **10000 записей**

## 3. Реализовано в репозитории
- Инфраструктура:
  - `docker-compose.yml` (PostgreSQL + Spark + ClickHouse)
  - `docker/postgres-init/01_create_raw.sql`
  - `docker/postgres-init/02_load_raw.sql`
  - `docker/spark/Dockerfile`
- Spark ETL:
  - `spark/etl_to_star_postgres.py` (этап `raw -> star`)
  - `spark/reports.py` (этап `star -> ClickHouse`, 6 витрин)
- Скрипты запуска и проверки:
  - `scripts/run_etl_to_star.sh`
  - `scripts/run_reports.sh`
  - `scripts/run_all.sh`
  - `scripts/check.sh`
  - `run_guide.md`

## 4. Выполненные шаги
1. Поднято окружение в Docker (PostgreSQL, Spark, ClickHouse).
2. Загружены все 10 CSV в staging-таблицу PostgreSQL.
3. Реализован и выполнен ETL `raw -> star` в PostgreSQL.
4. Реализовано и выполнено построение 6 витрин в ClickHouse.
5. Выполнена контрольная проверка результатов (`scripts/check.sh`).

## 5. Результаты проверки
### PostgreSQL (модель «звезда»)
- `stg_mock_data_raw = 10000`
- `fact_sales = 10000`
- `dim_customer = 1000`
- `dim_seller = 1000`
- `dim_product = 1000`
- `dim_store = 10000`
- `dim_supplier = 10000`
- `dim_date = 364`
- Пропуски внешних ключей в `fact_sales`: **0**

### ClickHouse (витрины)
- `report_sales_by_products = 1000`
- `report_sales_by_customers = 1000`
- `report_sales_by_time = 12`
- `report_sales_by_stores = 10000`
- `report_sales_by_suppliers = 10000`
- `report_product_quality = 1000`

## 6. Вывод
Лабораторная работа №2 выполнена: ETL-пайплайн на Apache Spark реализован, данные успешно преобразованы в модель «звезда» в PostgreSQL, аналитические витрины построены и загружены в ClickHouse, контрольные проверки пройдены.
