import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import pandas as pd

DB_USER = "postgres"
DB_PASS = "Linh@280405"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ecommerce"
path = "/home/nql/PycharmProjects/HadoopDataPipeline"

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


# --- 3. FUNCTION: INITIALIZE DB FUNCTIONS ---
# This runs once to ensure the 'update_updated_at_column' function exists in Postgres
def init_db_functions():
    print("Initializing Database Trigger Functions...")
    try:
        with engine.connect() as connection:
            sql = text("""
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = NOW();
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """)
            connection.execute(sql)
            connection.commit()
            print("✅ Trigger function 'update_updated_at_column' is ready.")
    except Exception as e:
        print(f"❌ Error initializing DB functions: {e}")


# --- 4. FUNCTION: ADD AUDIT COLUMNS & TRIGGERS ---
# This runs after every table load to inject the timestamps
def add_audit_columns(table_name):
    try:
        with engine.connect() as connection:
            # A. Add columns (Postgres auto-fills current time for existing rows)
            alter_sql = text(f"""
                ALTER TABLE public.{table_name}
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
            """)
            connection.execute(alter_sql)

            # B. Attach the Trigger for future updates
            trigger_sql = text(f"""
                DROP TRIGGER IF EXISTS set_updated_at ON public.{table_name};

                CREATE TRIGGER set_updated_at
                BEFORE UPDATE ON public.{table_name}
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();
            """)
            connection.execute(trigger_sql)
            connection.commit()
            print(f"   -> Added created_at/updated_at and triggers to '{table_name}'.")

    except Exception as e:
        print(f"   ❌ Failed to add audit columns to {table_name}: {e}")


# --- 5. FUNCTION: LOAD DATA ---
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

        # 2. Re-apply Schema Enhancements (Columns + Triggers)
        add_audit_columns(table_name)

    except Exception as e:
        print(f"❌ Error loading data: {e}")


# --- 6. MAIN EXECUTION FLOW ---
if __name__ == "__main__":

    # Step 1: Check Connection
    if test_connection():
        # Step 2: Setup Global Trigger Function (Run once)
        init_db_functions()

        print("\n--- Starting Data Load ---")
        load_csv(f"{path}/data/olist_geolocation_dataset.csv", "geolocation")
        load_csv(f"{path}/data/olist_customers_dataset.csv", "customers")
        load_csv(f"{path}/data/olist_products_dataset.csv", "products")
        load_csv(f"{path}/data/olist_sellers_dataset.csv", "sellers")
        load_csv(f"{path}/data/olist_orders_dataset.csv", "orders")
        load_csv(f"{path}/data/olist_order_items_dataset.csv", "order_items")
        load_csv(f"{path}/data/olist_order_payments_dataset.csv", "order_payments")
        load_csv(f"{path}/data/olist_order_reviews_dataset.csv", "order_reviews")

        print("\n✅ All operations completed.")
    else:
        print("\n❌ Aborting. Please fix your database credentials.")