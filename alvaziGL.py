"""
Alvazi micro GL processor

This program processes bank transaction CSV files, records the transactions in a General Ledger (GL) database,
and exports the GL items to an Excel sheet. It uses various modules to handle bank accounts, database operations,
GL items, and document processing.

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

# Main program logic
class Main:
    gldbFilePath: str
    bankAccountPropertiesFilePath: str
    alvaziGlDb: Database
    constants: Constants

    def __init__(self):
        self.constants = Constants('Configuration/constants.json')
        self.gldbFilePath = self.constants.get('gldbFilePath')
        self.bankAccountPropertiesFilePath = self.constants.get('bankAccountPropertiesFilePath')
        self.alvaziGlDb = Database(self.gldbFilePath)

    def refreshGlItemsTable(self):
        """
        Drops the GL items table and recreates it.
        """
        self.alvaziGlDb.drop_table(self.constants.get("gldbGlItemsTableName"))
        self.alvaziGlDb.create_gl_table(self.constants.get("gldbGlItemsTableName"))

    def closeGldb(self):
        """
        Closes the database connection.
        """
        self.alvaziGlDb.close()

    def processBankTransactionCsvFiles(self):
        # Use BankCSVIterator to iterate over CSV files and print bank account codes and file paths
        bankCsvIterator = BankCSVIterator(self.constants.get("bankFilesFolderPath"))
        for bankAccountCode, csvFilePath in bankCsvIterator:
            print(f"Bank Account Code: {bankAccountCode}, CSV File Path: {csvFilePath}")
            try:
                bankAccountProperties = BankAccount(
                    property_file_path=self.bankAccountPropertiesFilePath,
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
            try:
                gl_document = GLDocument(bank_transaction, bank_transactions.bank_account)
            except ValueError as e:
                print(f"Error processing transaction {index} / {bank_transaction.Amount} {bank_transaction.Description} : {e}")
                continue
            print(f"--- Processing transaction {index} {bank_transaction.Amount} {bank_transaction.Description} : {gl_document.items[1].currency_unit} / {gl_document.items[0].account_id} - {gl_document.items[1].account_id}")
            if not gl_document._gl_items_exist(self.alvaziGlDb):
                gl_document.insert_gl_items_into_db(self.alvaziGlDb)

    def write_gl_items_to_excel(self):
        """
        Reads the GL items from the database and adds them to a specified Excel sheet table.
        """
        excel_writer = GlToExcelWriter()
        excel_writer.write_gl_items_to_excel()

# Main program:
# Create Main object, refresh the database, process the bank transaction CSV files, and close the database connection

if __name__ == "__main__":
    main = Main()
    main.refreshGlItemsTable()
    main.processBankTransactionCsvFiles()
    main.closeGldb()
    main.write_gl_items_to_excel()

