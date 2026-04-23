Инструкция для запуска и проверки ЛР2. Все команды выполняются из корня проекта.

## 1. Состав проекта

- `docker-compose.yml` — сервисы PostgreSQL, ClickHouse, Spark.
- `docker/postgres-init/*.sql` — инициализация raw-таблицы и загрузка 10 CSV.

- `spark/etl_to_star_postgres.py` — ETL `raw -> star` в PostgreSQL.
- `spark/reports.py` — построение 6 витрин и загрузка в ClickHouse.

- `scripts/run_etl_to_star.sh` — запуск этапа `raw -> star`.
- `scripts/run_reports.sh` — запуск этапа `star -> ClickHouse`.
- `scripts/run_all.sh` — запуск обоих этапов подряд.

- `scripts/check.sh` — итоговая автоматическая проверка результата.

## 2. Подготовка окружения

Запуск/пересборка сервисов:

```bash
docker compose up -d --build
```

Порты:
- PostgreSQL: `localhost:5434`
- ClickHouse HTTP: `localhost:8124`

## 3. Проверка загрузки исходных данных

```bash
docker compose exec -T postgres psql -U bd_user -d bd_lab2 -c "SELECT count(*) AS raw_rows FROM bd_lab2.stg_mock_data_raw;"
```

Ожидаемое значение: `raw_rows = 10000`.

## 4. Запуск ETL

Поэтапно:

```bash
./scripts/run_etl_to_star.sh
./scripts/run_reports.sh
```

Или одним скриптом (выполняется один за другим):

```bash
./scripts/run_all.sh
```

## 5. Финальная проверка

```bash
./scripts/check.sh
```

Ожидаемые контольные результаты:
- `fact_sales = 10000`
- `dim_customer = 1000`
- `dim_seller = 1000`
- `dim_product = 1000`
- `dim_store = 10000`
- `dim_supplier = 10000`
- `dim_date = 364`
- во `fact_sales` нет `NULL` во внешних ключах
- в ClickHouse созданы и заполнены 6 витрин
