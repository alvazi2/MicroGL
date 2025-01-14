# Alvazi micro GL processor

# Libraries
import pandas as pd  # To read and represent csv files (bank exports)
from datetime import datetime  # To represent dates
import json  # To read JSON files
from decimal import Decimal  # To represent monetary values  https://docs.python.org/3/library/decimal.html
from dataclasses import dataclass
import hashlib # To hash transaction details
import sqlite3 # To store the GL data in a database

class BankAccount:
    def __init__(self, property_file_path: str, bank_account_code: str):
        self.json_file_path = property_file_path
        self.bank_account_code = bank_account_code
        self.properties = self._read_json_file()

    def _read_json_file(self):
        with open(self.json_file_path, 'r') as file:
            for bank_account_properties in json.load(file)['bankAccountProperties']:
                if bank_account_properties['bankAccountCode'] == self.bank_account_code:
                    return bank_account_properties
   
    def get_gl_mapping_for_search_string(self, search_string: str) -> dict:
        for determination in self.properties['glMapping']:
            if determination['searchString'] in search_string:
                return determination
        return {"message": "Determination not found"}
    

# Example usage
# bank_account_properties = BankAccountProperties('BankAccountProperties.json')
# print(bank_account_properties.properties)

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
type BankAccountCode = str  # The bank_account_codeal bank or credit card account that the transaction came from


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
        self._assign_transaction_id()
        self._add_gl_item()
        self._add_offsetting_gl_item()

    def _assign_transaction_id(self):
        """
        Assign a unique transaction ID by hashing the transaction details.
        """
        self.transaction_id = hashlib.sha256(f"{self.bank_transaction_record.Date}{self.bank_transaction_record.Amount}{self.bank_transaction_record.Description}".encode()).hexdigest()

    def _add_gl_item(self):
        """
        Create GL item from bank transaction, for the revenue or expense account. Add to the items list.
        Determination of D/C and sign of amount: 
            Credit for revenue or transfer in: record as negative amount - indicated by positive amount in bank file
            Debit for expenses or transfer out: record as positive amount - indicated by negative amount in bank file
        As a consequence, the amount here has the opposite sign of the bank file transaction amount.
        This method must be called before _add_offsetting_gl_item() as the latter is based on the outcome from here.
        """
        gl_mapping = self.bank_account.get_gl_mapping_for_search_string(self.bank_transaction_record.Description)
        # need errror handling if determination is not found

        if self.bank_transaction_record.Amount >= 0:
            # Credit for revenue or transfer in
            debit_credit_indicator = 'C'
        else:
            # Debit for expenses or transfer out
            debit_credit_indicator = 'D'

        # Check number if available...

        gl_item = GLItem(
            transaction_id = self.transaction_id,
            transaction_item_id = '001',
            transaction_date = self.bank_transaction_record.Date,
            posting_year = self.bank_transaction_record.Date.year,
            posting_period = self.bank_transaction_record.Date.month,
            transaction_amount = -Decimal(self.bank_transaction_record.Amount),
            currency_unit = self.bank_account.properties['currencyUnit'],
            debit_credit_indicator = debit_credit_indicator,
            transaction_description = self.bank_transaction_record.Description,
            account_id = gl_mapping['glAccount'],
            business_partner = gl_mapping['bp'],
            bank_account_code = self.bank_account.properties['bankAccountCode']
        )
        self.items.append(gl_item)

    def _add_offsetting_gl_item(self):
        """
        Create offsetting GL item from bank transaction, for the bank balance sheet account. Add to the items list.
        Determination of D/C and sign of amount: 
            Debit for deposit to bank account: record as positive amount (revenue or transfer in)
            Credit for withdrawal from bank account: record as negative amount (expense or transfer out)
        """
        offsetting_transaction_item_id = '002'
        offsetting_transaction_amount = -self.items[0].transaction_amount
        offsetting_debit_credit_indicator = 'C' if self.items[0].debit_credit_indicator == 'D' else 'D'
        offsetting_account_id = self.bank_account.properties['balanceSheetAccount']

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
            bank_account_code=self.items[0].bank_account_code
        )
        self.items.append(offsetting_gl_item)

    def insert_gl_items_into_db(self, db_path: str):
        """
        Inserts the GL items into the SQLite database.
        
        Args:
            db_path (str): The path to the SQLite database file.
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        for item in self.items:
            cursor.execute('''
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
            ''', (
                item.transaction_id,
                item.transaction_item_id,
                item.transaction_date,
                item.posting_year,
                item.posting_period,
                float(item.transaction_amount),
                item.currency_unit,
                item.debit_credit_indicator,
                item.transaction_description,
                item.account_id,
                item.business_partner,
                item.bank_account_code
            ))
        conn.commit()
        conn.close()

    def _gl_items_exist(self, db_path: str) -> bool:
        """
        Checks if GL items for the current transaction ID already exist in the database.
        
        Args:
            db_path (str): The path to the SQLite database file.
        
        Returns:
            bool: True if GL items exist, False otherwise.
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM gl_items WHERE transaction_id = ?
        ''', (self.transaction_id,))
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0

class BankCSVReader:
    def __init__(self, bank_account_code: str, csv_file_path: str, bank_account: BankAccount):
        self.bank_account_code = bank_account_code
        self.csv_file_path = csv_file_path
        self.bank_account = bank_account
        self.bank_transaction_records = self._read_csv_file()

    def _read_csv_file(self):
        print(self.bank_account.properties['csvFileHasHeader']) 
        print(self.bank_account.properties['csvFileColumnTitles']) 
        print(self.bank_account.properties['csvFileColumns'])
        if self.bank_account.properties['csvFileHasHeader']:
            return pd.read_csv(self.csv_file_path, 
                           usecols=self.bank_account.properties['csvFileColumns'],
                           names=self.bank_account.properties['csvFileColumnTitles'],
                           header=0,
                           index_col=None, 
                           parse_dates=['Date'] 
                           )
        else:
            return pd.read_csv(self.csv_file_path, 
                           usecols=self.bank_account.properties['csvFileColumns'],
                           names=self.bank_account.properties['csvFileColumnTitles'],
                           header=None,
                           index_col=None, 
                           parse_dates=['Date'] 
                           )

# Temporary: db setup
def create_gl_table(db_path: str):
    """
    Creates the GL table in the SQLite database.
    
    Args:
        db_path (str): The path to the SQLite database file.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS gl_items (
            transaction_id TEXT,
            transaction_item_id TEXT,
            transaction_date TEXT,
            posting_year INTEGER,
            posting_period INTEGER,
            transaction_amount REAL,
            currency_unit TEXT,
            debit_credit_indicator TEXT,
            transaction_description TEXT,
            account_id TEXT,
            business_partner TEXT,
            bank_account_code TEXT
        )
    ''')
    conn.commit()
    conn.close()

if 1 == 2: create_gl_table('alvaziGL-Data/alvaziGL.db')

# test the process for a single bank transaction record
wfc_bank_account_properties = BankAccount(property_file_path='BankAccountProperties.json', bank_account_code='WFC')
wfc_bank_transactions = BankCSVReader('WFC', 'Bank-Files/WFC-test.csv', wfc_bank_account_properties)
print(wfc_bank_transactions.bank_transaction_records)

for index, bank_transaction in wfc_bank_transactions.bank_transaction_records.iterrows():
    gl_document = GLDocument(bank_transaction, wfc_bank_account_properties)
    #gl_document.insert_gl_items_into_db(db_path)
    print(f"Processing transaction {index}")
    print(gl_document.items)
    if index >= 20:
        break   # Just to limit the output

# Thoughts: read bank account properties and filter for specific bank account code
# BankCSVReader only deals with the filtered bank account details
# Goal is to have then a main class that completely processes a bank transaction CSV file
# Need to also improve / enable error handling