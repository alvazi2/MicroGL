"""
GL Processor

This module contains the GLProcessor class which processes bank transaction CSV files, records the transactions
in a General Ledger (GL) database, and exports the GL items to an Excel sheet. It uses various modules to handle
bank accounts, database operations, GL items, and document processing.

Modules:
- bank_account: For handling bank account properties.
- database: For database operations.
- gl_item: For GL item handling.
- gl_document: For GL document processing.
- bank_csv_reader: For reading and iterating over bank transaction CSV files.
- GlToExcelWriter: For writing GL items to an Excel sheet.
"""

# Libraries
from bank_account import BankAccount
from database import Database
from gl_item import GLItem
from gl_document import GLDocument
from bank_csv_reader import BankCSVIterator, BankCSVReader
from gl_to_excel_writer import GlToExcelWriter
from constants import Constants
from chart_of_accounts import ChartOfAccounts

class GLProcessor:
    gldb_file_path: str
    bank_account_properties_file_path: str
    alvazi_gl_db: Database
    constants: Constants
    chart_of_accounts: ChartOfAccounts

    def __init__(self):
        self.constants = Constants('Configuration/constants.json')
        self.gldb_file_path = self.constants.get('gldbFilePath')
        self.bank_account_properties_file_path = self.constants.get('bankAccountPropertiesFilePath')
        self.alvazi_gl_db = Database(self.gldb_file_path)
        self.chart_of_accounts = ChartOfAccounts('Configuration/ChartOfAccounts.json')

    def refresh_gl_items_table(self):
        """
        Drops the GL items table and recreates it.
        """
        self.alvazi_gl_db.drop_table(self.constants.get("gldbGlItemsTableName"))
        self.alvazi_gl_db.create_gl_table(self.constants.get("gldbGlItemsTableName"))

    def close_gldb(self):
        """
        Closes the database connection.
        """
        self.alvazi_gl_db.close()

    def process_bank_transaction_csv_files(self):
        # Use BankCSVIterator to iterate over CSV files and print bank account codes and file paths
        bankCsvIterator = BankCSVIterator(self.constants.get("bankFilesFolderPath"))
        for bankAccountCode, csvFilePath in bankCsvIterator:
            print(f"Bank Account Code: {bankAccountCode}, CSV File Path: {csvFilePath}")
            try:
                bankAccountProperties = BankAccount(
                    property_file_path=self.bank_account_properties_file_path,
                    bank_account_code=bankAccountCode
                )
            except ValueError as e:
                print(f"Error: {e}")
                continue
            bankTransactions = BankCSVReader(
                bank_account_code = bankAccountCode, 
                csv_file_path = csvFilePath, 
                bank_account = bankAccountProperties
                )
            self._record_bank_file_transactions_in_GL(bankTransactions)

    def _record_bank_file_transactions_in_GL(self, bank_transactions: BankCSVReader):
        for (
            index,
            bank_transaction,
        ) in bank_transactions.bank_transaction_records.iterrows():
            
            print(f"--- Processing transaction {bank_transaction.CSVFile} {bank_transaction.RowIndex}: "
                  f"{bank_transaction.Amount} {bank_transaction.Description}")
        
            try:
                gl_document = GLDocument(
                    bank_transaction, 
                    bank_transactions.bank_account, 
                    self.chart_of_accounts, 
                    self.constants  # Pass constants to GLDocument
                )
            except ValueError as e:
                print(f"Error processing transaction {index} / {bank_transaction.Amount} {bank_transaction.Description} : {e}")
                continue

            print(f"     Recording transaction {bank_transaction.CSVFile} {bank_transaction.RowIndex}: "
                  f"{bank_transaction.Amount} {bank_transaction.Description} : "
                  f"{gl_document.items[1].currency_unit} / "
                  f"{gl_document.items[0].account_id} - {gl_document.items[1].account_id}")
        
            if not gl_document._gl_items_exist(self.alvazi_gl_db):
                gl_document.insert_gl_items_into_db(self.alvazi_gl_db)
            else:
                print(f"Transaction {bank_transaction.CSVFile} {bank_transaction.RowIndex} is already in the database.")

    def write_gl_items_to_excel(self):
        """
        Reads the GL items from the database and adds them to a specified Excel sheet table.
        """
        excel_writer = GlToExcelWriter()
        excel_writer.write_gl_items_to_excel()
