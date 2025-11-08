import os
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus
import io

# Load environment variables
load_dotenv()

# Get credentials from .env
KOBO_USERNAME = os.getenv('KOBO_USERNAME')
KOBO_PASSWORD = os.getenv('KOBO_PASSWORD')
KOBO_URL = "https://kf.kobotoolbox.org/api/v2/assets/aNc6mWL63tg9r9HY3FZ2RC/export-settings/esX4EPJ2oqFPWp2gatc2boE/data.csv"

SQL_HOST = os.getenv('SQL_HOST')
SQL_PORT = os.getenv('SQL_PORT')
SQL_USERNAME = os.getenv('SQL_USERNAME')
SQL_PASSWORD = os.getenv('SQL_PASSWORD')
SQL_DATABASE = os.getenv('SQL_DATABASE')

# Debug: Print what we're reading (hide password)
print("=== Environment Variables Debug ===")
print(f"SQL_HOST: '{SQL_HOST}'")
print(f"SQL_PORT: '{SQL_PORT}'")
print(f"SQL_USERNAME: '{SQL_USERNAME}'")
print(f"SQL_PASSWORD: '{SQL_PASSWORD[:2]}***' (length: {len(SQL_PASSWORD) if SQL_PASSWORD else 0})")
print(f"SQL_DATABASE: '{SQL_DATABASE}'")
print("===================================\n")

def fetch_kobo_data():
    """Fetch data from KoboToolbox"""
    try:
        response = requests.get(
            KOBO_URL,
            auth=(KOBO_USERNAME, KOBO_PASSWORD),
            timeout=30
        )
        response.raise_for_status()
        
        # Read CSV directly from response text using semicolon delimiter
        csv_data = io.StringIO(response.text)
        df = pd.read_csv(csv_data, sep=';', on_bad_lines='skip')
        
        print(f"Data fetched successfully! Shape: {df.shape}")
        print(f"\nColumn names found:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. '{col}'")
        
        print(f"\nFirst 3 rows:")
        print(df.head(3))
        print("\n" + "="*50 + "\n")
        
        return df
    
    except Exception as e:
        print(f"Error fetching data: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_database_connection():
    """Create SQLAlchemy engine with proper password encoding"""
    try:
        # Strip whitespace and quotes from all values
        host = SQL_HOST.strip().strip('"').strip("'") if SQL_HOST else "localhost"
        port = SQL_PORT.strip().strip('"').strip("'") if SQL_PORT else "5432"
        username = SQL_USERNAME.strip().strip('"').strip("'") if SQL_USERNAME else "postgres"
        password = SQL_PASSWORD.strip().strip('"').strip("'") if SQL_PASSWORD else ""
        database = SQL_DATABASE.strip().strip('"').strip("'") if SQL_DATABASE else ""
        
        print(f"Attempting connection to: {username}@{host}:{port}/{database}")
        
        # URL encode the password to handle special characters
        encoded_password = quote_plus(password)
        encoded_username = quote_plus(username)
        
        connection_string = f"postgresql://{encoded_username}:{encoded_password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        print("Database connection established!")
        return engine
    
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("\nTroubleshooting tips:")
        print("1. Verify your PostgreSQL password is correct")
        print("2. Check if PostgreSQL is running on localhost:5432")
        print("3. Verify user 'postgres' exists and has access to database 'Chidinma'")
        return None

def create_schema_and_table(engine):
    """Create schema and table"""
    try:
        with engine.connect() as conn:
            # Create schema if it doesn't exist
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS feedback_ghana;"))
            conn.commit()
            print("Schema 'feedback_ghana' created/verified!")
            
            # Drop table if exists (optional - remove if you want to keep existing data)
            conn.execute(text("DROP TABLE IF EXISTS feedback_ghana.customer_feedback CASCADE;"))
            conn.commit()
            
            # Create table
            create_table_query = """
            CREATE TABLE feedback_ghana.customer_feedback (
                id SERIAL PRIMARY KEY,
                start_time TIMESTAMP WITH TIME ZONE,
                end_time TIMESTAMP WITH TIME ZONE,
                date_of_reporting DATE,
                store_location VARCHAR(255),
                gender VARCHAR(50),
                age INTEGER,
                product_pricing_satisfaction INTEGER,
                customer_service_satisfaction INTEGER,
                overall_satisfaction INTEGER,
                recommendations TEXT,
                submission_id BIGINT,
                uuid VARCHAR(255),
                submission_time TIMESTAMP,
                validation_status VARCHAR(100),
                notes TEXT,
                status VARCHAR(100),
                submitted_by VARCHAR(255),
                version VARCHAR(100),
                tags TEXT,
                index_value INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_table_query))
            conn.commit()
            print("Table 'customer_feedback' created successfully!")
            
    except Exception as e:
        print(f"Error creating schema/table: {e}")
        raise

def clean_and_insert_data(df, engine):
    """Clean data and insert into PostgreSQL"""
    try:
        print("=== Starting Data Cleaning ===")
        
        # Rename columns to match database schema
        df_clean = df.rename(columns={
            'start': 'start_time',
            'end': 'end_time',
            'Date of reporting': 'date_of_reporting',
            'Store Location': 'store_location',
            'Gender': 'gender',
            'Age': 'age',
            'How satisfy are you with the product pricing': 'product_pricing_satisfaction',
            'How satified are you with the customers services': 'customer_service_satisfaction',
            'What is your overall satisfaction': 'overall_satisfaction',
            'What are your recommendations': 'recommendations',
            '_id': 'submission_id',
            '_uuid': 'uuid',
            '_submission_time': 'submission_time',
            '_validation_status': 'validation_status',
            '_notes': 'notes',
            '_status': 'status',
            '_submitted_by': 'submitted_by',
            '__version__': 'version',
            '_tags': 'tags',
            '_index': 'index_value'
        })
        
        print(f"Columns after renaming: {list(df_clean.columns)[:5]}...")
        
        # Select only the columns we need
        columns_to_insert = [
            'start_time', 'end_time', 'date_of_reporting', 'store_location',
            'gender', 'age', 'product_pricing_satisfaction', 
            'customer_service_satisfaction', 'overall_satisfaction',
            'recommendations', 'submission_id', 'uuid', 'submission_time',
            'validation_status', 'notes', 'status', 'submitted_by',
            'version', 'tags', 'index_value'
        ]
        
        df_clean = df_clean[columns_to_insert]
        
        # Convert date columns
        df_clean['start_time'] = pd.to_datetime(df_clean['start_time'], errors='coerce')
        df_clean['end_time'] = pd.to_datetime(df_clean['end_time'], errors='coerce')
        df_clean['date_of_reporting'] = pd.to_datetime(df_clean['date_of_reporting'], errors='coerce').dt.date
        df_clean['submission_time'] = pd.to_datetime(df_clean['submission_time'], errors='coerce')
        
        # Convert numeric columns
        df_clean['age'] = pd.to_numeric(df_clean['age'], errors='coerce')
        df_clean['product_pricing_satisfaction'] = pd.to_numeric(df_clean['product_pricing_satisfaction'], errors='coerce')
        df_clean['customer_service_satisfaction'] = pd.to_numeric(df_clean['customer_service_satisfaction'], errors='coerce')
        df_clean['overall_satisfaction'] = pd.to_numeric(df_clean['overall_satisfaction'], errors='coerce')
        df_clean['submission_id'] = pd.to_numeric(df_clean['submission_id'], errors='coerce').astype('Int64')
        df_clean['index_value'] = pd.to_numeric(df_clean['index_value'], errors='coerce')
        
        print(f"\nData preview before insertion:")
        print(df_clean.head())
        print(f"\nData types:")
        print(df_clean.dtypes)
        
        # Insert data
        df_clean.to_sql(
            name='customer_feedback',
            schema='feedback_ghana',
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        print(f"\n✓ Successfully inserted {len(df_clean)} rows into feedback_ghana.customer_feedback!")
        
    except Exception as e:
        print(f"Error inserting data: {e}")
        import traceback
        traceback.print_exc()
        raise

def main():
    """Main execution function"""
    print("Starting data pipeline...")
    print("="*50 + "\n")
    
    # Step 1: Fetch data from Kobo
    df = fetch_kobo_data()
    if df is None or df.empty:
        print("No data fetched. Exiting.")
        return
    
    # Step 2: Create database connection
    engine = create_database_connection()
    if engine is None:
        return
    
    # Step 3: Create schema and table
    create_schema_and_table(engine)
    
    # Step 4: Clean and insert data
    clean_and_insert_data(df, engine)
    
    # Step 5: Verify insertion
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM feedback_ghana.customer_feedback;"))
        count = result.scalar()
        print(f"\n{'='*50}")
        print(f"✓ Total records in table: {count}")
        print(f"{'='*50}")
        
        # Show sample data
        if count > 0:
            print("\nSample data from database:")
            sample = conn.execute(text("""
                SELECT id, date_of_reporting, store_location, gender, age, 
                       overall_satisfaction, created_at
                FROM feedback_ghana.customer_feedback 
                ORDER BY id 
                LIMIT 3;
            """))
            for row in sample:
                print(row)
    
    print("\n✓ Data pipeline completed successfully!")

if __name__ == "__main__":
    main()