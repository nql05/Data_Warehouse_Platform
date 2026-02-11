import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime

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


def create_update_trigger_function():
    """Create a reusable function for updating the updated_at column."""
    print("\nCreating update trigger function...")
    try:
        with engine.begin() as connection:
            connection.execute(text("""
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """))
        print("   -> Trigger function created successfully.")
    except Exception as e:
        print(f"❌ Error creating trigger function: {e}")


def add_timestamp_columns_and_trigger(table_name):
    """Add created_at and updated_at columns and create trigger for updates."""
    print(f"\nAdding timestamp columns to '{table_name}'...")
    try:
        with engine.begin() as connection:
            # Add created_at column with default value
            connection.execute(text(f"""
                ALTER TABLE {table_name} 
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
            """))

            # Add updated_at column with default value
            connection.execute(text(f"""
                ALTER TABLE {table_name} 
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
            """))

            # Drop existing trigger if it exists
            connection.execute(text(f"""
                DROP TRIGGER IF EXISTS update_{table_name}_updated_at ON {table_name};
            """))

            # Create trigger for updated_at
            connection.execute(text(f"""
                CREATE TRIGGER update_{table_name}_updated_at
                BEFORE UPDATE ON {table_name}
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();
            """))

        print(f"   -> Timestamp columns and trigger added to '{table_name}'.")
    except Exception as e:
        print(f"❌ Error adding timestamp columns to '{table_name}': {e}")


def load_csv(file_path, table_name):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    print(f"\nLoading {file_path} into table '{table_name}'...")
    try:
        df = pd.read_csv(file_path)

        # Write Data with CASCADE to handle dependencies
        with engine.begin() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE;"))

        df.to_sql(
            table_name,
            engine,
            if_exists='replace',
            index=False
        )
        print(f"   -> Successfully loaded {len(df)} rows.")

        # Add timestamp columns and trigger after loading data
        add_timestamp_columns_and_trigger(table_name)

    except Exception as e:
        print(f"❌ Error loading data: {e}")


if __name__ == "__main__":
    if test_connection():
        print("\n--- Starting Data Load ---")

        # Create the reusable trigger function once
        create_update_trigger_function()

        # Load in correct order: parent tables first, then child tables
        # Parent tables (no foreign keys)
        load_csv(os.path.join(DATA_DIR, "olist_geolocation_dataset.csv"), "geolocation")
        load_csv(os.path.join(DATA_DIR, "olist_customers_dataset.csv"), "customers")
        load_csv(os.path.join(DATA_DIR, "olist_products_dataset.csv"), "products")
        load_csv(os.path.join(DATA_DIR, "olist_sellers_dataset.csv"), "sellers")

        # Child tables (depend on parent tables)
        load_csv(os.path.join(DATA_DIR, "olist_orders_dataset.csv"), "orders")
        load_csv(os.path.join(DATA_DIR, "olist_order_items_dataset.csv"), "order_items")
        load_csv(os.path.join(DATA_DIR, "olist_order_payments_dataset.csv"), "order_payments")
        load_csv(os.path.join(DATA_DIR, "olist_order_reviews_dataset.csv"), "order_reviews")

        print("\n✅ All operations completed.")
    else:
        print("\n❌ Aborting. Please fix your database credentials.")