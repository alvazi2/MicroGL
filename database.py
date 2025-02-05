import sqlite3
from decimal import Decimal  # To represent monetary values, https://docs.python.org/3/library/decimal.html
from datetime import datetime

# Adapter functions to map decimals to integers for storage in SQLite
def adapt_decimal(d):
    return int(d * 100)

def convert_decimal(i):
    return Decimal(i.decode('utf-8')) / 100

def adapt_date(date):
    return date.strftime("%Y-%m-%d")

def convert_date(date_str):
    return datetime.strptime(date_str.decode('utf-8'), "%Y-%m-%d")

class Database:
    db_path: str
    connection: sqlite3.Connection
    cursor: sqlite3.Cursor

    def __init__(self, db_path: str):
        if not db_path:
            raise ValueError("db_path must be a valid string representing the path to the database file.")
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.cursor = self.connection.cursor()
        
        # Register the adapter and converter for transaction amount
        sqlite3.register_adapter(Decimal, adapt_decimal)
        sqlite3.register_converter("DECIMAL", convert_decimal)

        # Register the adapter and converter for transaction date
        sqlite3.register_adapter(datetime, adapt_date)
        sqlite3.register_converter("DATE", convert_date)
    
    def commit(self):
        self.connection.commit()
    
    def close(self):
        self.connection.close()

    def drop_table(self, table_name: str):
        self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.connection.commit()

    def create_gl_table(self, table_name: str):
        self.cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                transaction_id TEXT,
                transaction_item_id TEXT,
                transaction_date DATE,
                posting_year INTEGER,
                posting_period INTEGER,
                transaction_amount DECIMAL, 
                currency_unit TEXT,
                debit_credit_indicator TEXT,
                transaction_description TEXT,
                account_id TEXT,
                business_partner TEXT,
                bank_account_code TEXT,
                investment_name TEXT,
                investment_symbol TEXT,
                check_no TEXT,
                PRIMARY KEY (transaction_id, transaction_item_id)
            )
            """
        )
        self.connection.commit()
