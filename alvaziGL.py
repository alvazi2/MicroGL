# Alvazi micro GL processor

# Libraries
import pandas as pd  # To read and represent csv files (bank exports)
import json  # To read JSON files
from decimal import Decimal  # To represent monetary values  https://docs.python.org/3/library/decimal.html
from dataclasses import dataclass
import hashlib # To hash transaction details
import sqlite3 # To store the GL data in a database

class BankAccountProperties:
    def __init__(self, json_file_path: str):
        self.json_file_path = json_file_path
        self.properties = self._read_json_file()

    def _read_json_file(self):
        with open(self.json_file_path, 'r') as file:
            return json.load(file)

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
type Origin = str  # The original bank or credit card account that the transaction came from


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
    origin: Origin


# Represent the complete GL document as a list of GL items
type GLItemList = list[GLItem]

@dataclass
class GLDocument:
    items: GLItemList
    row: pd.Series

    def __init__(self, row: pd.Series):
        self.row = row
        self.items = []
        self._assign_transaction_id()
        self._fill_gl_item()

    def _assign_transaction_id(self):
        self.transaction_id = hashlib.sha256(f"{self.row.Date}{self.row.Amount}{self.row.Description}".encode()).hexdigest()

    def _fill_gl_item(self):
        transaction_item_id = 1
        transaction_date = self.row.Date
        posting_year = int(transaction_date.split('-')[0])
        posting_period = int(transaction_date.split('-')[1])
        transaction_amount = Decimal(self.row.Amount)
        currency_unit = 'USD'
        debit_credit_indicator = 'D' if self.row.Amount >= 0 else 'C'
        transaction_description = self.row.Description
        account_id = "TBD"  # This should be determined based on your logic
        business_partner = "TBD"  # This should be determined based on your logic
        origin = "TBD"  # This should be determined based on your logic

        gl_item = GLItem(
            transaction_id=self.transaction_id,
            transaction_item_id=transaction_item_id,
            transaction_date=transaction_date,
            posting_year=posting_year,
            posting_period=posting_period,
            transaction_amount=transaction_amount,
            currency_unit=currency_unit,
            debit_credit_indicator=debit_credit_indicator,
            transaction_description=transaction_description,
            account_id=account_id,
            business_partner=business_partner,
            origin=origin
        )
        self.items.append(gl_item)

# Example usage
# Assuming df is your DataFrame and you are iterating over its rows
# for index, row in df.iterrows():
#     gl_document = GLDocument(row)
#     print(gl_document.items)

class BankCSVReader:
    def __init__(self, bank_account_short_code: str, csv_file_path: str, bank_account_properties: BankAccountProperties):
        self.bank_account_short_code = bank_account_short_code
        self.csv_file_path = csv_file_path
        self.bank_account_properties = bank_account_properties
        self.bank_transaction_records = self._read_csv_file()

    def _read_csv_file(self):
        #columns = self.bank_account_properties.properties[self.bank_account_short_code]['columns']
        #columns_to_read = self.bank_account_properties.properties
        for bank_account_details in self.bank_account_properties.properties['bankAccountProperties']:
            if bank_account_details['bankAccountShortCode'] == self.bank_account_short_code:
                print(bank_account_details['csvFileHasHeader']) 
                print(bank_account_details['csvFileColumnTitles']) 
                print(bank_account_details['csvFileColumns']) 
            break

        return pd.read_csv(self.csv_file_path, 
                           usecols=bank_account_details['csvFileColumns'],
                           names=bank_account_details['csvFileColumnTitles'],
                           header=0,
                           index_col=None, parse_dates=True
                           ) if bank_account_details['csvFileHasHeader'] else pd.read_csv(self.csv_file_path, 
                           usecols=bank_account_details['csvFileColumns'],
                           names=bank_account_details['csvFileColumnTitles'],
                           header=None,
                           index_col=None, parse_dates=True
                           )
        

# Example usage
bank_account_properties = BankAccountProperties('BankAccountProperties.json')
csv_reader = BankCSVReader('WFC', 'Bank-Files/WFC-test.csv', bank_account_properties)
print(csv_reader.df)

# Thoughts: read bank account properties and filter for specific bank account short code
# BankCSVReader only deals with the filtered bank account details
# Goal is to have then a master class that completely processes a bank transaction CSV file