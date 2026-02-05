import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import pandas as pd

DB_USER = "postgres"
DB_PASS = "Linh@280405"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ecommerce"

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Go up one level from src/ to project root, then into data/
DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "data")

# Connection String
DATABASE_URL = f"postgresql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, future=True)


# --- 2. FUNCTION: TEST CONNECTION ---
def test_connection():
    print(f"Testing connection to database '{DB_NAME}'...")
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            print(f"✅ SUCCESS! Connected to: {version}")
            return True
    except Exception as e:
        print(f"❌ CONNECTION FAILED: {e}")
        return False

def load_csv(file_path, table_name):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    print(f"\nLoading {file_path} into table '{table_name}'...")
    try:
        df = pd.read_csv(file_path)

        # 1. Write Data (Replaces table, destroys schema)
        df.to_sql(
            table_name,
            engine,
            if_exists='replace',
            index=False
        )
        print(f"   -> Successfully loaded {len(df)} rows.")

    except Exception as e:
        print(f"❌ Error loading data: {e}")


# --- 6. MAIN EXECUTION FLOW ---
if __name__ == "__main__":

    # Step 1: Check Connection
    if test_connection():

        print("\n--- Starting Data Load ---")
        load_csv(os.path.join(DATA_DIR, "olist_orders_dataset.csv"), "orders")
        load_csv(os.path.join(DATA_DIR, "olist_order_items_dataset.csv"), "order_items")
        load_csv(os.path.join(DATA_DIR, "olist_order_payments_dataset.csv"), "order_payments")
        load_csv(os.path.join(DATA_DIR, "olist_order_reviews_dataset.csv"), "order_reviews")
        load_csv(os.path.join(DATA_DIR, "olist_customers_dataset.csv"), "customers")
        load_csv(os.path.join(DATA_DIR, "olist_products_dataset.csv"), "products")
        load_csv(os.path.join(DATA_DIR, "olist_sellers_dataset.csv"), "sellers")
        load_csv(os.path.join(DATA_DIR, "olist_geolocation_dataset.csv"), "geolocation")

        print("\n✅ All operations completed.")
    else:
        print("\n❌ Aborting. Please fix your database credentials.")