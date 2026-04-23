import argparse

import clickhouse_connect
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def extract_single_value(query_result):
    value = query_result.first_item
    if isinstance(value, dict):
        return next(iter(value.values()))
    if isinstance(value, (list, tuple)):
        return value[0] if value else None
    return value


def ch_write_df(client, df, table_name: str, create_sql: str, jdbc_url: str, jdbc_props: dict):
    client.command(f"DROP TABLE IF EXISTS {table_name}")
    client.command(create_sql)

    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", jdbc_props["user"])
        .option("password", jdbc_props["password"])
        .option("driver", jdbc_props["driver"])
        .option("batchsize", "50000")
        .mode("append")
        .save()
    )

    rows_count = extract_single_value(client.query(f"SELECT count() FROM {table_name}"))
    print(f"{table_name}: rows={rows_count}")


def main():
    parser = argparse.ArgumentParser(description="Spark ETL: star schema PostgreSQL -> report marts ClickHouse")
    parser.add_argument("--pg-host", default="postgres")
    parser.add_argument("--pg-port", default="5432")
    parser.add_argument("--pg-db", default="bd_lab2")
    parser.add_argument("--pg-user", default="bd_user")
    parser.add_argument("--pg-password", default="bd_pass")
    parser.add_argument("--pg-schema", default="bd_lab2")
    parser.add_argument("--pg-driver", default="org.postgresql.Driver")

    parser.add_argument("--ch-host", default="clickhouse")
    parser.add_argument("--ch-port", type=int, default=8123)
    parser.add_argument("--ch-user", default="bd_ch_user")
    parser.add_argument("--ch-password", default="bd_ch_pass")
    parser.add_argument("--ch-database", default="bd_lab2")
    parser.add_argument("--ch-driver", default="com.clickhouse.jdbc.ClickHouseDriver")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("bd-lab2-reports-to-clickhouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"
    jdbc_props = {
        "user": args.pg_user,
        "password": args.pg_password,
        "driver": args.pg_driver,
    }

    def read_table(table_name: str):
        return (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"{args.pg_schema}.{table_name}")
            .options(**jdbc_props)
            .load()
        )

    fact = read_table("fact_sales")
    d_customer = read_table("dim_customer")
    d_product = read_table("dim_product")
    d_store = read_table("dim_store")
    d_supplier = read_table("dim_supplier")
    d_date = read_table("dim_date")

    base = (
        fact.alias("f")
        .join(d_product.alias("p"), F.col("f.product_key") == F.col("p.product_key"), "left")
        .join(d_customer.alias("c"), F.col("f.customer_key") == F.col("c.customer_key"), "left")
        .join(d_store.alias("st"), F.col("f.store_key") == F.col("st.store_key"), "left")
        .join(d_supplier.alias("su"), F.col("f.supplier_key") == F.col("su.supplier_key"), "left")
        .join(d_date.alias("dd"), F.col("f.sale_date_key") == F.col("dd.date_key"), "left")
        .select(
            F.col("f.source_row_id"),
            F.col("f.sale_quantity"),
            F.col("f.sale_total_price"),
            F.col("f.unit_price"),
            F.col("f.product_price"),
            F.col("f.product_rating"),
            F.col("f.product_reviews"),
            F.col("p.product_key"),
            F.col("p.product_name"),
            F.col("p.product_category"),
            F.col("c.customer_key"),
            F.col("c.customer_first_name"),
            F.col("c.customer_last_name"),
            F.col("c.customer_country"),
            F.col("st.store_key"),
            F.col("st.store_name"),
            F.col("st.store_city"),
            F.col("st.store_country"),
            F.col("su.supplier_key"),
            F.col("su.supplier_name"),
            F.col("su.supplier_country"),
            F.col("dd.month_num"),
            F.col("dd.month_name"),
            F.col("dd.year_num"),
        )
    )

    # 1) Report by products
    product_metrics = (
        base.groupBy("product_key", "product_name", "product_category")
        .agg(
            F.sum("sale_quantity").alias("total_sales_qty"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("product_rating"), 2).alias("avg_rating"),
            F.max("product_reviews").cast("long").alias("total_reviews"),
        )
    )
    category_revenue = (
        base.groupBy("product_category")
        .agg(F.round(F.sum("sale_total_price"), 2).alias("category_revenue"))
    )
    w_prod = Window.orderBy(F.col("total_sales_qty").desc(), F.col("total_revenue").desc())
    r_products = (
        product_metrics.join(category_revenue, "product_category", "left")
        .withColumn("sales_rank", F.row_number().over(w_prod))
        .select(
            "sales_rank",
            "product_key",
            "product_name",
            "product_category",
            "total_sales_qty",
            "total_revenue",
            "category_revenue",
            "avg_rating",
            "total_reviews",
        )
    )

    # 2) Report by customers
    customer_metrics = (
        base.groupBy("customer_key", "customer_first_name", "customer_last_name", "customer_country")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_spent"),
            F.count("source_row_id").alias("orders_count"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
        )
    )
    country_distribution = (
        d_customer.groupBy("customer_country")
        .agg(F.countDistinct("customer_key").alias("customers_in_country"))
    )
    w_cust = Window.orderBy(F.col("total_spent").desc())
    r_customers = (
        customer_metrics.join(country_distribution, "customer_country", "left")
        .withColumn("spending_rank", F.row_number().over(w_cust))
        .withColumn("customer_full_name", F.concat_ws(" ", F.col("customer_first_name"), F.col("customer_last_name")))
        .select(
            "spending_rank",
            "customer_key",
            "customer_full_name",
            "customer_country",
            "customers_in_country",
            "orders_count",
            "total_spent",
            "avg_check",
        )
    )

    # 3) Report by time
    r_time = (
        base.groupBy("year_num", "month_num", "month_name")
        .agg(
            F.count("source_row_id").alias("orders_count"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_order_size"),
        )
        .orderBy("year_num", "month_num")
    )
    w_time = Window.orderBy("year_num", "month_num")
    r_time = (
        r_time.withColumn("prev_period_revenue", F.lag("total_revenue").over(w_time))
        .withColumn(
            "revenue_change_vs_prev",
            F.round(F.col("total_revenue") - F.col("prev_period_revenue"), 2),
        )
        .withColumn(
            "year_revenue",
            F.round(F.sum("total_revenue").over(Window.partitionBy("year_num")), 2),
        )
        .select(
            "year_num",
            "month_num",
            "month_name",
            "orders_count",
            "total_revenue",
            "avg_order_size",
            "prev_period_revenue",
            "revenue_change_vs_prev",
            "year_revenue",
        )
    )

    # 4) Report by stores
    w_store = Window.orderBy(F.col("total_revenue").desc())
    r_stores = (
        base.groupBy("store_key", "store_name", "store_city", "store_country")
        .agg(
            F.count("source_row_id").alias("orders_count"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
        )
        .withColumn("revenue_rank", F.row_number().over(w_store))
        .withColumn("is_top5", (F.col("revenue_rank") <= 5).cast("int"))
        .select(
            "revenue_rank",
            "is_top5",
            "store_key",
            "store_name",
            "store_city",
            "store_country",
            "orders_count",
            "total_revenue",
            "avg_check",
        )
    )

    # 5) Report by suppliers
    w_supplier = Window.orderBy(F.col("total_revenue").desc())
    r_suppliers = (
        base.groupBy("supplier_key", "supplier_name", "supplier_country")
        .agg(
            F.count("source_row_id").alias("orders_count"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("product_price"), 2).alias("avg_product_price"),
        )
        .withColumn("revenue_rank", F.row_number().over(w_supplier))
        .withColumn("is_top5", (F.col("revenue_rank") <= 5).cast("int"))
        .select(
            "revenue_rank",
            "is_top5",
            "supplier_key",
            "supplier_name",
            "supplier_country",
            "orders_count",
            "total_revenue",
            "avg_product_price",
        )
    )

    # 6) Product quality report
    product_quality = (
        base.groupBy("product_key", "product_name", "product_category")
        .agg(
            F.round(F.avg("product_rating"), 2).alias("avg_rating"),
            F.sum("sale_quantity").alias("total_sales_qty"),
            F.max("product_reviews").cast("long").alias("total_reviews"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        )
    )
    corr_value = product_quality.select(F.corr("avg_rating", "total_sales_qty").alias("corr_val")).first()["corr_val"]
    corr_value = float(corr_value) if corr_value is not None else 0.0

    w_rating_desc = Window.orderBy(F.col("avg_rating").desc(), F.col("total_reviews").desc())
    w_rating_asc = Window.orderBy(F.col("avg_rating").asc(), F.col("total_reviews").desc())
    w_reviews = Window.orderBy(F.col("total_reviews").desc(), F.col("avg_rating").desc())

    r_quality = (
        product_quality.withColumn("rating_rank_desc", F.dense_rank().over(w_rating_desc))
        .withColumn("rating_rank_asc", F.dense_rank().over(w_rating_asc))
        .withColumn("reviews_rank", F.dense_rank().over(w_reviews))
        .withColumn("is_highest_rated", (F.col("rating_rank_desc") == 1).cast("int"))
        .withColumn("is_lowest_rated", (F.col("rating_rank_asc") == 1).cast("int"))
        .withColumn("is_most_reviewed", (F.col("reviews_rank") == 1).cast("int"))
        .withColumn("rating_sales_corr", F.lit(corr_value))
        .select(
            "product_key",
            "product_name",
            "product_category",
            "avg_rating",
            "total_sales_qty",
            "total_reviews",
            "total_revenue",
            "rating_sales_corr",
            "is_highest_rated",
            "is_lowest_rated",
            "is_most_reviewed",
        )
    )

    admin_client = clickhouse_connect.get_client(
        host=args.ch_host,
        port=args.ch_port,
        username=args.ch_user,
        password=args.ch_password,
        database="default",
    )
    admin_client.command(f"CREATE DATABASE IF NOT EXISTS {args.ch_database}")

    client = admin_client
    if args.ch_database != "default":
        admin_client.close()
        client = clickhouse_connect.get_client(
            host=args.ch_host,
            port=args.ch_port,
            username=args.ch_user,
            password=args.ch_password,
            database=args.ch_database,
        )

    ch_jdbc_url = f"jdbc:clickhouse://{args.ch_host}:{args.ch_port}/{args.ch_database}"
    ch_jdbc_props = {
        "user": args.ch_user,
        "password": args.ch_password,
        "driver": args.ch_driver,
    }

    ch_write_df(
        client,
        r_products,
        "report_sales_by_products",
        """
        CREATE TABLE report_sales_by_products (
            sales_rank UInt32,
            product_key UInt32,
            product_name String,
            product_category String,
            total_sales_qty Int64,
            total_revenue Decimal(18,2),
            category_revenue Decimal(18,2),
            avg_rating Decimal(9,2),
            total_reviews Int64
        ) ENGINE = MergeTree ORDER BY (sales_rank, product_key)
        """,
        ch_jdbc_url,
        ch_jdbc_props,
    )

    ch_write_df(
        client,
        r_customers,
        "report_sales_by_customers",
        """
        CREATE TABLE report_sales_by_customers (
            spending_rank UInt32,
            customer_key UInt32,
            customer_full_name String,
            customer_country String,
            customers_in_country Int64,
            orders_count Int64,
            total_spent Decimal(18,2),
            avg_check Decimal(18,2)
        ) ENGINE = MergeTree ORDER BY (spending_rank, customer_key)
        """,
        ch_jdbc_url,
        ch_jdbc_props,
    )

    ch_write_df(
        client,
        r_time,
        "report_sales_by_time",
        """
        CREATE TABLE report_sales_by_time (
            year_num UInt32,
            month_num UInt32,
            month_name String,
            orders_count Int64,
            total_revenue Decimal(18,2),
            avg_order_size Decimal(18,2),
            prev_period_revenue Nullable(Decimal(18,2)),
            revenue_change_vs_prev Nullable(Decimal(18,2)),
            year_revenue Decimal(18,2)
        ) ENGINE = MergeTree ORDER BY (year_num, month_num)
        """,
        ch_jdbc_url,
        ch_jdbc_props,
    )

    ch_write_df(
        client,
        r_stores,
        "report_sales_by_stores",
        """
        CREATE TABLE report_sales_by_stores (
            revenue_rank UInt32,
            is_top5 UInt8,
            store_key UInt32,
            store_name String,
            store_city String,
            store_country String,
            orders_count Int64,
            total_revenue Decimal(18,2),
            avg_check Decimal(18,2)
        ) ENGINE = MergeTree ORDER BY (revenue_rank, store_key)
        """,
        ch_jdbc_url,
        ch_jdbc_props,
    )

    ch_write_df(
        client,
        r_suppliers,
        "report_sales_by_suppliers",
        """
        CREATE TABLE report_sales_by_suppliers (
            revenue_rank UInt32,
            is_top5 UInt8,
            supplier_key UInt32,
            supplier_name String,
            supplier_country String,
            orders_count Int64,
            total_revenue Decimal(18,2),
            avg_product_price Decimal(18,2)
        ) ENGINE = MergeTree ORDER BY (revenue_rank, supplier_key)
        """,
        ch_jdbc_url,
        ch_jdbc_props,
    )

    ch_write_df(
        client,
        r_quality,
        "report_product_quality",
        """
        CREATE TABLE report_product_quality (
            product_key UInt32,
            product_name String,
            product_category String,
            avg_rating Decimal(9,2),
            total_sales_qty Int64,
            total_reviews Int64,
            total_revenue Decimal(18,2),
            rating_sales_corr Float64,
            is_highest_rated UInt8,
            is_lowest_rated UInt8,
            is_most_reviewed UInt8
        ) ENGINE = MergeTree ORDER BY (product_key)
        """,
        ch_jdbc_url,
        ch_jdbc_props,
    )

    print("=== REPORTS LOADED TO CLICKHOUSE ===")
    spark.stop()


if __name__ == "__main__":
    main()
