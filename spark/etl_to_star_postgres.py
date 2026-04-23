import argparse
from functools import reduce
from typing import List

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def clean_str(col_name: str):
    return F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(F.trim(F.col(col_name)))


def to_int(col_name: str):
    c = clean_str(col_name)
    return F.when(c.rlike(r"^[0-9]+$"), c.cast("int"))


def to_decimal(col_name: str, precision: int = 12, scale: int = 2):
    c = clean_str(col_name)
    return F.when(c.rlike(r"^[0-9]+(\.[0-9]+)?$"), c.cast(f"decimal({precision},{scale})"))


def to_date_mdY(col_name: str):
    c = clean_str(col_name)
    return F.when(c.rlike(r"^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$"), F.to_date(c, "M/d/yyyy"))


def add_surrogate_key(df, key_col: str, natural_cols: List[str]):
    sort_cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in natural_cols]
    w = Window.orderBy(*sort_cols)
    return df.withColumn(key_col, F.row_number().over(w))


def latest_by_keys(src_df, key_cols: List[str], attrs: List[str]):
    w = Window.partitionBy(*key_cols).orderBy(F.col("stg_id").desc())
    select_cols = ["stg_id"] + key_cols + [c for c in attrs if c not in key_cols]
    return (
        src_df.where(reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in key_cols]))
        .select(*select_cols)
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn", "stg_id")
    )


def main():
    parser = argparse.ArgumentParser(description="Spark ETL: raw PostgreSQL -> star schema PostgreSQL")
    parser.add_argument("--pg-host", default="postgres")
    parser.add_argument("--pg-port", default="5432")
    parser.add_argument("--pg-db", default="bd_lab2")
    parser.add_argument("--pg-user", default="bd_user")
    parser.add_argument("--pg-password", default="bd_pass")
    parser.add_argument("--pg-schema", default="bd_lab2")
    parser.add_argument("--pg-driver", default="org.postgresql.Driver")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("bd-lab2-etl-to-star")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"
    jdbc_props = {
        "user": args.pg_user,
        "password": args.pg_password,
        "driver": args.pg_driver,
    }

    raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"{args.pg_schema}.stg_mock_data_raw")
        .options(**jdbc_props)
        .load()
    )

    src = raw_df.select(
        F.col("stg_id").cast("long").alias("stg_id"),
        clean_str("source_id").alias("source_id"),
        clean_str("customer_first_name").alias("customer_first_name"),
        clean_str("customer_last_name").alias("customer_last_name"),
        to_int("customer_age").alias("customer_age"),
        clean_str("customer_email").alias("customer_email"),
        clean_str("customer_country").alias("customer_country"),
        clean_str("customer_postal_code").alias("customer_postal_code"),
        clean_str("customer_pet_type").alias("customer_pet_type"),
        clean_str("customer_pet_name").alias("customer_pet_name"),
        clean_str("customer_pet_breed").alias("customer_pet_breed"),
        clean_str("pet_category").alias("pet_category"),
        clean_str("seller_first_name").alias("seller_first_name"),
        clean_str("seller_last_name").alias("seller_last_name"),
        clean_str("seller_email").alias("seller_email"),
        clean_str("seller_country").alias("seller_country"),
        clean_str("seller_postal_code").alias("seller_postal_code"),
        clean_str("product_name").alias("product_name"),
        clean_str("product_category").alias("product_category"),
        clean_str("product_brand").alias("product_brand"),
        clean_str("product_material").alias("product_material"),
        clean_str("product_color").alias("product_color"),
        clean_str("product_size").alias("product_size"),
        clean_str("product_description").alias("product_description"),
        to_decimal("product_weight", 10, 2).alias("product_weight"),
        to_decimal("product_price", 12, 2).alias("product_price"),
        to_decimal("product_rating", 4, 2).alias("product_rating"),
        to_int("product_reviews").alias("product_reviews"),
        to_date_mdY("product_release_date").alias("product_release_date"),
        to_date_mdY("product_expiry_date").alias("product_expiry_date"),
        to_int("product_quantity").alias("product_quantity"),
        to_date_mdY("sale_date").alias("sale_date"),
        to_int("sale_customer_id").alias("source_sale_customer_id"),
        to_int("sale_seller_id").alias("source_sale_seller_id"),
        to_int("sale_product_id").alias("source_sale_product_id"),
        to_int("sale_quantity").alias("sale_quantity"),
        to_decimal("sale_total_price", 12, 2).alias("sale_total_price"),
        clean_str("store_name").alias("store_name"),
        clean_str("store_location").alias("store_location"),
        clean_str("store_city").alias("store_city"),
        clean_str("store_state").alias("store_state"),
        clean_str("store_country").alias("store_country"),
        clean_str("store_phone").alias("store_phone"),
        clean_str("store_email").alias("store_email"),
        clean_str("supplier_name").alias("supplier_name"),
        clean_str("supplier_contact").alias("supplier_contact"),
        clean_str("supplier_email").alias("supplier_email"),
        clean_str("supplier_phone").alias("supplier_phone"),
        clean_str("supplier_address").alias("supplier_address"),
        clean_str("supplier_city").alias("supplier_city"),
        clean_str("supplier_country").alias("supplier_country"),
    ).cache()

    customer_attrs = [
        "customer_first_name",
        "customer_last_name",
        "customer_age",
        "customer_email",
        "customer_country",
        "customer_postal_code",
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed",
        "pet_category",
    ]
    dim_customer = latest_by_keys(src, ["source_sale_customer_id"], customer_attrs)
    dim_customer = dim_customer.withColumnRenamed("source_sale_customer_id", "source_customer_id")
    dim_customer = add_surrogate_key(dim_customer, "customer_key", ["source_customer_id"])

    seller_attrs = [
        "seller_first_name",
        "seller_last_name",
        "seller_email",
        "seller_country",
        "seller_postal_code",
    ]
    dim_seller = latest_by_keys(src, ["source_sale_seller_id"], seller_attrs)
    dim_seller = dim_seller.withColumnRenamed("source_sale_seller_id", "source_seller_id")
    dim_seller = add_surrogate_key(dim_seller, "seller_key", ["source_seller_id"])

    product_attrs = [
        "product_name",
        "product_category",
        "product_brand",
        "product_material",
        "product_color",
        "product_size",
        "product_description",
        "product_weight",
        "product_release_date",
        "product_expiry_date",
    ]
    dim_product = latest_by_keys(src, ["source_sale_product_id"], product_attrs)
    dim_product = dim_product.withColumnRenamed("source_sale_product_id", "source_product_id")
    dim_product = add_surrogate_key(dim_product, "product_key", ["source_product_id"])

    store_attrs = [
        "store_location",
        "store_city",
        "store_state",
        "store_phone",
        "store_email",
    ]
    store_key_cols = [
        "store_name",
        "store_location",
        "store_city",
        "store_country",
        "store_phone",
        "store_email",
    ]
    dim_store = latest_by_keys(src, store_key_cols, store_attrs)
    dim_store = add_surrogate_key(dim_store, "store_key", store_key_cols)

    supplier_attrs = [
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
    ]

    supplier_key_cols = [
        "supplier_name",
        "supplier_email",
        "supplier_phone",
        "supplier_country",
    ]
    dim_supplier = latest_by_keys(src, supplier_key_cols, supplier_attrs)
    dim_supplier = add_surrogate_key(dim_supplier, "supplier_key", supplier_key_cols)

    dim_date = (
        src.select(F.col("sale_date").alias("full_date"))
        .where(F.col("full_date").isNotNull())
        .dropDuplicates()
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("month_num", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter_num", F.quarter("full_date"))
        .withColumn("year_num", F.year("full_date"))
        .withColumn("week_num", F.weekofyear("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .select(
            "date_key",
            "full_date",
            "day_of_month",
            "month_num",
            "month_name",
            "quarter_num",
            "year_num",
            "week_num",
            "day_of_week",
            "day_name",
        )
    )

    s = src.alias("s")
    c = dim_customer.alias("c")
    se = dim_seller.alias("se")
    p = dim_product.alias("p")
    st = dim_store.alias("st")
    su = dim_supplier.alias("su")
    d = dim_date.alias("d")

    fact = (
        s.join(c, F.col("s.source_sale_customer_id") == F.col("c.source_customer_id"), "left")
        .join(se, F.col("s.source_sale_seller_id") == F.col("se.source_seller_id"), "left")
        .join(p, F.col("s.source_sale_product_id") == F.col("p.source_product_id"), "left")
        .join(
            st,
            reduce(lambda a, b: a & b, [F.col(f"s.{c}") == F.col(f"st.{c}") for c in store_key_cols]),
            "left",
        )
        .join(
            su,
            reduce(lambda a, b: a & b, [F.col(f"s.{c}") == F.col(f"su.{c}") for c in supplier_key_cols]),
            "left",
        )
        .join(d, F.col("s.sale_date") == F.col("d.full_date"), "left")
        .select(
            F.col("s.stg_id").alias("source_row_id"),
            F.col("d.date_key").alias("sale_date_key"),
            F.col("c.customer_key").alias("customer_key"),
            F.col("se.seller_key").alias("seller_key"),
            F.col("p.product_key").alias("product_key"),
            F.col("st.store_key").alias("store_key"),
            F.col("su.supplier_key").alias("supplier_key"),
            F.col("s.source_sale_customer_id"),
            F.col("s.source_sale_seller_id"),
            F.col("s.source_sale_product_id"),
            F.col("s.sale_quantity"),
            F.col("s.sale_total_price"),
            F.col("s.product_price"),
            F.col("s.product_rating"),
            F.col("s.product_reviews"),
            F.when(
                (F.col("s.sale_quantity") > 0) & F.col("s.sale_total_price").isNotNull(),
                F.round(F.col("s.sale_total_price") / F.col("s.sale_quantity"), 2),
            ).alias("unit_price"),
        )
    )

    write_opts = {
        "url": jdbc_url,
        "user": args.pg_user,
        "password": args.pg_password,
        "driver": args.pg_driver,
    }

    def write_table(df, table_name: str):
        (
            df.write.format("jdbc")
            .options(**write_opts)
            .option("dbtable", f"{args.pg_schema}.{table_name}")
            .mode("overwrite")
            .save()
        )

    write_table(dim_customer.select("customer_key", "source_customer_id", *customer_attrs), "dim_customer")
    write_table(dim_seller.select("seller_key", "source_seller_id", *seller_attrs), "dim_seller")
    write_table(dim_product.select("product_key", "source_product_id", *product_attrs), "dim_product")
    write_table(dim_store.select("store_key", "store_name", "store_country", *store_attrs), "dim_store")
    write_table(dim_supplier.select("supplier_key", "supplier_name", "supplier_country", *supplier_attrs), "dim_supplier")
    write_table(dim_date, "dim_date")
    write_table(fact, "fact_sales")

    print("=== ETL FINISHED ===")
    print(f"raw_rows={src.count()}")
    print(f"dim_customer={dim_customer.count()}")
    print(f"dim_seller={dim_seller.count()}")
    print(f"dim_product={dim_product.count()}")
    print(f"dim_store={dim_store.count()}")
    print(f"dim_supplier={dim_supplier.count()}")
    print(f"dim_date={dim_date.count()}")
    print(f"fact_sales={fact.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
