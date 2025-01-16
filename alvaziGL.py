# Alvazi micro GL processor

# Libraries
import pandas as pd  # To read and represent csv files (bank exports)
from datetime import datetime  # To represent dates
import json  # To read JSON files
from decimal import (
    Decimal,
)  # To represent monetary values  https://docs.python.org/3/library/decimal.html
from dataclasses import dataclass
import os
import hashlib  # To hash transaction details
import sqlite3  # To store the GL data in a database

# Constants
BANK_TRANSACTION_CATEGORIES = {"Deposit": "D", "Withdrawal": "C"}
DC_INDICATORS = {"Debit": "D", "Credit": "C"}
GLDB_FILE_PATH = "alvaziGL-Data/alvaziGL.db"
BANK_ACCOUNT_PROPERTIES_FILE_PATH = "BankAccountProperties.json"

# Functions

# Adapter functions to map decimals to integers for storage in SQLite

# Adapter to convert Decimal to integer (scaled by 100)
def adapt_decimal(d):
    return int(d * 100)

# Converter to convert integer back to Decimal (scaled by 100)
def convert_decimal(i):
    return Decimal(i) / 100

# Adapter to convert datetime to string (YYYY-MM-DD)
def adapt_date(date):
    return date.strftime("%Y-%m-%d")

# Converter to convert string (YYYY-MM-DD) back to datetime
def convert_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")


# Classes

class BankAccount:
    def __init__(self, property_file_path: str, bank_account_code: str):
        self.json_file_path = property_file_path
        self.bank_account_code = bank_account_code
        self.properties = self._read_bank_account_properties()

    def _read_bank_account_properties(self):
        with open(self.json_file_path, "r") as file:
            for bank_account_properties in json.load(file)["bankAccountProperties"]:
                if bank_account_properties["bankAccountCode"] == self.bank_account_code:
                    return bank_account_properties
        raise ValueError(f"No properties found for bank account code: {self.bank_account_code}")

    def get_gl_mapping_for_search_string(self, search_string: str, bank_transaction_category: str) -> dict:
        """
        Get the GL mapping for a search string.
        """
        for gl_mapping in self.properties["glMapping"]:
            if gl_mapping["searchString"] in search_string:
                return gl_mapping

        # If no GL mapping found, return a default determination
        if bank_transaction_category == BANK_TRANSACTION_CATEGORIES["Deposit"]:
            return {
                    "searchString": search_string,
                    "glAccount": self.properties["missingGlMappingDefault"]["glAccountRevenue"],
                    "bp": self.properties["missingGlMappingDefault"]["unknownBp"]
                    }
        else:
            return {
                    "searchString": search_string,
                    "glAccount": self.properties["missingGlMappingDefault"]["glAccountExpense"],
                    "bp": self.properties["missingGlMappingDefault"]["unknownBp"]
                    }            


class Database:
    db_path: str
    connection: sqlite3.Connection
    cursor: sqlite3.Cursor

    def __init__(self, db_path: str):
        """
        Initializes the Database Connection with a given database path.

        Args:
            db_path (str): The path to the SQLite database file.
        """
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path)
        self.cursor = self.connection.cursor()
    
    def commit(self):
        """
        Commits the current transaction.
        """
        self.connection.commit()
    
    def close(self):
        """
        Closes the database connection.
        """
        self.connection.close()


# Temporary: db setup
def create_gl_table(db_path: str):
    """
    Creates the GL table in the SQLite database.
    Store transaction amounts with Python data type Decimal as integer (scaled by 100) via adapter

    Args:
        db_path (str): The path to the SQLite database file.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS gl_items (
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
            bank_account_code TEXT
        )
    """
    )
    conn.commit()
    conn.close()


# Data types for GL data (GL document items)
type TransactionID = str  # Unique identifier for each transaction - TBD / maybe a hash of the transaction details?
type TransactionItemID = int  # Item number within transaction document
type TransactionDate = str  # ... to be determined how to handle this
type PostingYear = int  # The year in which the transaction was posted
type PostingPeriod = int  # The period in which the transaction was posted
type TransactionAmount = Decimal  #
type CurrencyUnit = str
type DebitCreditIndicator = str  # 'D' or 'C'
type TransactionDescription = str
type AccountID = str
type BusinessPartner = str
type BankAccountCode = str  # Code for bank or credit card account that the transaction came from

@dataclass
class GLItem:
    transaction_id: TransactionID
    transaction_item_id: TransactionItemID
    transaction_date: TransactionDate
    posting_year: PostingYear
    posting_period: PostingPeriod
    transaction_amount: TransactionAmount
    currency_unit: CurrencyUnit
    debit_credit_indicator: DebitCreditIndicator
    transaction_description: TransactionDescription
    account_id: AccountID
    business_partner: BusinessPartner
    bank_account_code: BankAccountCode


# Represent the complete GL document as a list of GL items
type GLItemList = list[GLItem]


# @dataclass
class GLDocument:
    bank_transaction_record: pd.Series
    bank_account: BankAccount
    items: GLItemList

    def __init__(self, bank_transaction_record: pd.Series, bank_account: BankAccount):
        """
        Initializes the GLDocument with a bank transaction record and a bank account.

        Args:
            bank_transaction_record (pd.Series): The bank transaction record.
            bank_account (BankAccount): The bank account associated with the transaction (property dictionary).
        """
        self.bank_transaction_record = bank_transaction_record
        self.bank_account = bank_account
        self.items = []
        self.bank_transaction_category = self._determine_bank_transaction_category()
        self._assign_transaction_id()
        self._add_gl_item()
        self._add_offsetting_gl_item()

    def _determine_bank_transaction_category(self) -> str:
        """
        Determines the category of the bank transaction (Deposit or Withdrawal).

        Returns:
            str: The category of the bank transaction.
        """
        if self.bank_transaction_record.Amount >= 0:
            return BANK_TRANSACTION_CATEGORIES["Deposit"]
        else:
            return BANK_TRANSACTION_CATEGORIES["Withdrawal"]
        
    def _assign_transaction_id(self):
        """
        Assign a unique transaction ID by hashing the transaction details.
        """
        self.transaction_id = hashlib.sha256(
            f"{self.bank_transaction_record.Date}{self.bank_transaction_record.Amount}{self.bank_transaction_record.Description}".encode()
        ).hexdigest()

    def _add_gl_item(self):
        """
        Create GL item from bank transaction, for the revenue or expense account. Add to the items list.
        Determination of D/C and sign of amount:
            Credit for revenue or transfer in: record as negative amount - indicated by positive amount in bank file
            Debit for expenses or transfer out: record as positive amount - indicated by negative amount in bank file
        As a consequence, the amount here has the opposite sign of the bank file transaction amount.
        This method must be called before _add_offsetting_gl_item() as the latter is based on the outcome from here.
        """
        gl_mapping = self.bank_account.get_gl_mapping_for_search_string(
            self.bank_transaction_record.Description, 
            self.bank_transaction_category
        )
        # Need errror handling if determination is not found:
        # get_gl_mapping_for_search_string to raise an error message if mapping not found, then catch here.

        if self.bank_transaction_record.Amount >= 0:
            # Credit for revenue or transfer in
            debit_credit_indicator = DC_INDICATORS["Credit"]
        else:
            # Debit for expenses or transfer out
            debit_credit_indicator = DC_INDICATORS["Debit"]

        # Check number if available...

        gl_item = GLItem(
            transaction_id=self.transaction_id,
            transaction_item_id="001",
            transaction_date=self.bank_transaction_record.Date.to_pydatetime(),
            posting_year=self.bank_transaction_record.Date.year,
            posting_period=self.bank_transaction_record.Date.month,
            transaction_amount=-Decimal(self.bank_transaction_record.Amount),
            currency_unit=self.bank_account.properties["currencyUnit"],
            debit_credit_indicator=debit_credit_indicator,
            transaction_description=self.bank_transaction_record.Description,
            account_id=gl_mapping["glAccount"],
            business_partner=gl_mapping["bp"],
            bank_account_code=self.bank_account.properties["bankAccountCode"],
        )
        self.items.append(gl_item)
        print(f"Transaction amount: {self.bank_transaction_record.Amount}")

    def _add_offsetting_gl_item(self):
        """
        Create offsetting GL item from bank transaction, for the bank balance sheet account. Add to the items list.
        Determination of D/C and sign of amount:
            Debit for deposit to bank account: record as positive amount (revenue or transfer in)
            Credit for withdrawal from bank account: record as negative amount (expense or transfer out)
        """
        offsetting_transaction_item_id = "002"
        offsetting_transaction_amount = -self.items[0].transaction_amount
        offsetting_debit_credit_indicator = (
            DC_INDICATORS["Credit"] if self.items[0].debit_credit_indicator == DC_INDICATORS["Debit"] else DC_INDICATORS["Debit"]
        )
        offsetting_account_id = self.bank_account.properties["balanceSheetAccount"]

        offsetting_gl_item = GLItem(
            transaction_id=self.transaction_id,
            transaction_item_id=offsetting_transaction_item_id,
            transaction_date=self.items[0].transaction_date,
            posting_year=self.items[0].posting_year,
            posting_period=self.items[0].posting_period,
            transaction_amount=offsetting_transaction_amount,
            currency_unit=self.items[0].currency_unit,
            debit_credit_indicator=offsetting_debit_credit_indicator,
            transaction_description=self.items[0].transaction_description,
            account_id=offsetting_account_id,
            business_partner=self.items[0].business_partner,
            bank_account_code=self.items[0].bank_account_code,
        )
        self.items.append(offsetting_gl_item)

    def insert_gl_items_into_db(self, glDb: Database):
        """
        Inserts the GL items into the SQLite database.

        Args:
            glDb (Database): database object for the GL database
        """
        for item in self.items:
            glDb.cursor.execute(
                """
                INSERT INTO gl_items (
                    transaction_id,
                    transaction_item_id,
                    transaction_date,
                    posting_year,
                    posting_period,
                    transaction_amount,
                    currency_unit,
                    debit_credit_indicator,
                    transaction_description,
                    account_id,
                    business_partner,
                    bank_account_code
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    item.transaction_id,
                    item.transaction_item_id,
                    item.transaction_date,
                    item.posting_year,
                    item.posting_period,
                    item.transaction_amount, # Store as integer (scaled by 100) via adapter
                    item.currency_unit,
                    item.debit_credit_indicator,
                    item.transaction_description,
                    item.account_id,
                    item.business_partner,
                    item.bank_account_code,
                ),
            )
        glDb.commit()

    def _gl_items_exist(self, glDb: Database) -> bool:
        """
        Checks if GL items for the current transaction ID already exist in the database.

        Args:
            glDb (Database): database object for the GL database

        Returns:
            bool: True if GL items exist, False otherwise.
        """
        glDb.cursor.execute(
            """
            SELECT COUNT(*) FROM gl_items WHERE transaction_id = ?
        """,
            (self.transaction_id,),
        )
        count = glDb.cursor.fetchone()[0]
        return count > 0

class BankCSVIterator:
    def __init__(self, folder_path: str):
        self.folder_path = folder_path
        self.files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.index < len(self.files):
            file_name = self.files[self.index]
            self.index += 1
            bank_account_code = file_name.split('-')[0]  # Assuming the bank account code is the first part of the filename
            return bank_account_code, os.path.join(self.folder_path, file_name)
        else:
            raise StopIteration

class BankCSVReader:
    def __init__(
        self, bank_account_code: str, csv_file_path: str, bank_account: BankAccount
    ):
        self.bank_account_code = bank_account_code
        self.csv_file_path = csv_file_path
        self.bank_account = bank_account
        self.bank_transaction_records = self._read_csv_file()

    def _read_csv_file(self):
        print(self.bank_account.properties["csvFileHasHeader"])
        print(self.bank_account.properties["csvFileColumnTitles"])
        print(self.bank_account.properties["csvFileColumns"])
        if self.bank_account.properties["csvFileHasHeader"]:
            return pd.read_csv(
                self.csv_file_path,
                usecols=self.bank_account.properties["csvFileColumns"],
                names=self.bank_account.properties["csvFileColumnTitles"],
                header=0,
                index_col=None,
                parse_dates=["Date"],
            )
        else:
            return pd.read_csv(
                self.csv_file_path,
                usecols=self.bank_account.properties["csvFileColumns"],
                names=self.bank_account.properties["csvFileColumnTitles"],
                header=None,
                index_col=None,
                parse_dates=["Date"],
            )


class Main:
    gldb_file_path: str
    bank_account_properties_file_path: str
    alvaziGl_db: Database

    def __init__(self, gldb_file_path: str, bank_account_properties_file_path: str):
        self.gldb_file_path = gldb_file_path
        self.bank_account_properties_file_path = bank_account_properties_file_path

        # Initialize the database (connection object)
        self.alvaziGl_db = Database(GLDB_FILE_PATH)

        # Register the adapter and converter for transaction amount
        sqlite3.register_adapter(Decimal, adapt_decimal)
        sqlite3.register_converter("DECIMAL", convert_decimal)

        # Register the adapter and converter for transaction date
        sqlite3.register_adapter(datetime, adapt_date)
        sqlite3.register_converter("DATE", convert_date)

    def close(self):
        """
        Closes the database connection.
        """
        self.alvaziGl_db.close()

    def process_bank_transaction_csv_files(self):
        # Use BankCSVIterator to iterate over CSV files and print bank account codes and file paths
        bank_csv_iterator = BankCSVIterator("Bank-Files")
        for bank_account_code, csv_file_path in bank_csv_iterator:
            print(f"Bank Account Code: {bank_account_code}, CSV File Path: {csv_file_path}")
            try:
                bank_account_properties = BankAccount(
                    property_file_path=self.bank_account_properties_file_path,
                    bank_account_code=bank_account_code
                )
            except ValueError as e:
                print(f"Error: {e}")
                continue
            bank_transactions = BankCSVReader(
                bank_account_code = bank_account_code, 
                csv_file_path = csv_file_path, 
                bank_account = bank_account_properties
                )
            self._record_bank_file_transactions_in_GL(bank_transactions)

    def _record_bank_file_transactions_in_GL(self, bank_transactions: BankCSVReader):
        for (
            index,
            bank_transaction,
        ) in bank_transactions.bank_transaction_records.iterrows():
            gl_document = GLDocument(bank_transaction, bank_transactions.bank_account)
            print(f"---------- Processing transaction {index} : {bank_transaction.Amount} {gl_document.items[1].currency_unit}")
            # print(gl_document.items)
            if not gl_document._gl_items_exist(self.alvaziGl_db):
                gl_document.insert_gl_items_into_db(self.alvaziGl_db)
            #if index >= 20:
            #    break



if 1 == 2:
    create_gl_table(GLDB_FILE_PATH)

main_program = Main(GLDB_FILE_PATH, BANK_ACCOUNT_PROPERTIES_FILE_PATH)
main_program.process_bank_transaction_csv_files()
main_program.close()

