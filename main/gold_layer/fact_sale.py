from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, coalesce, lit, avg

# --- CONFIGURATION ---
SILVER_PATH = "hdfs://localhost:9000/datalake/silver"
GOLD_PATH = "hdfs://localhost:9000/datalake/gold"

def prepare_reviews(df_reviews):
    print(" --- Aggregating Reviews (One Score per Order) ---")
    return df_reviews.groupBy("order_id").agg(
        avg("review_score").cast("int").alias("review_score")
    )

def create_fact_sales(df_items, df_orders, df_reviews_agg):
    print(" --- Joining Items + Orders + Reviews ---")
    # Explicit Aliasing for safety
    i = df_items.alias("i")
    o = df_orders.alias("o")
    r = df_reviews_agg.alias("r")
    # Join Logic:
    # 1. Left Join Orders: We need the Date/Customer from Orders.
    # 2. Left Join Reviews: We attach the score (if it exists).
    df_joined = i.join(o, col("i.order_id") == col("o.order_id"), "left") \
        .join(r, col("i.order_id") == col("r.order_id"), "left")
    return df_joined


def apply_business_rules(df):
    """
    Transforms the joined data into the final Fact structure.
    - Generates Keys (Surrogate Keys handling)
    - Calculates Metrics (Revenue)
    - Handles Nulls (Dimension Integrity)
    """
    print(" --- Applying business rules and calculating metrics ---")
    return df.select(
        # --- Composite Primary Key ---
        col("i.order_id"),
        col("i.order_item_id"),
        # --- Dimension Foreign Keys (Handle Nulls -> 'Unknown') ---
        # Used to join with Dim_Product, Dim_Seller, Dim_Customer
        coalesce(col("i.product_id")).alias("product_key"),
        coalesce(col("i.seller_id")).alias("seller_key"),
        coalesce(col("o.customer_id")).alias("customer_key"),
        # --- Time Dimension Key (Integer YYYYMMDD) ---
        # Used to join with Dim_Date. Default to 19900101 if date is missing.
        coalesce(
            date_format(col("o.order_purchase_timestamp"), "yyyyMMdd"),
            lit("19900101")
        ).cast("int").alias("date_key"),
        # Keep timestamp for precise drill-down if needed
        col("o.order_purchase_timestamp"),
        # --- Facts / Metrics (Handle Nulls -> 0) ---
        coalesce(col("i.price")).alias("sales_amount"),
        coalesce(col("i.freight_value")).alias("freight_value"),
        # Calculated Metric: Total Transaction Value
        (coalesce(col("i.price")) + coalesce(col("i.freight_value"))).alias("total_amount"),
        # --- Attributes ---
        # If score is missing, use -1 to indicate "Not Reviewed"
        coalesce(col("r.review_score")).alias("review_score"),
        col("o.order_status")
    )

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Fact_Sale_Table") \
        .master("local[*]") \
        .getOrCreate()

    df_items = spark.read.parquet(f"{SILVER_PATH}/order_items")
    df_orders = spark.read.parquet(f"{SILVER_PATH}/orders")
    df_reviews = spark.read.parquet(f"{SILVER_PATH}/order_reviews")

    # 2. Pre-process Reviews (The Fan-out Fix)
    df_reviews_agg = prepare_reviews(df_reviews)
    # 3. Create Fact (Join)
    df_joined = create_fact_sales(df_items, df_orders, df_reviews_agg)
    # 4. Apply Rules (Transform)
    df_fact = apply_business_rules(df_joined)
    # 5. Deduplicate (Final Safety Net)
    # Since we aggregated reviews, this is just a backup check
    df_final = df_fact.dropDuplicates(["order_id", "order_item_id"])
    # 6. Write to Gold
    df_final.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_sales")
    print("SUCCESS!!!")
    spark.stop()