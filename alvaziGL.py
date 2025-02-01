# Alvazi micro GL processor

# Libraries
import json
from bank_account import BankAccount
from database import Database
from gl_item import GLItem
from gl_document import GLDocument
from bank_csv_reader import BankCSVIterator, BankCSVReader

# Config class to load constants from JSON file
class Config:
    def __init__(self, config_file_path: str):
        with open(config_file_path, 'r') as file:
            self.config = json.load(file)
    
    def get(self, key: str):
        if key not in self.config:
            raise KeyError(f"Key '{key}' not found in configuration.")
        return self.config[key]

# Main program logic
class Main:
    gldbFilePath: str
    bankAccountPropertiesFilePath: str
    alvaziGlDb: Database
    config: Config

    def __init__(self):
        self.config = Config('constants.json')
        self.gldbFilePath = self.config.get('gldbFilePath')
        self.bankAccountPropertiesFilePath = self.config.get('bankAccountPropertiesFilePath')
        self.alvaziGlDb = Database(self.gldbFilePath)

    def refreshGlItemsTable(self):
        """
        Drops the GL items table and recreates it.
        """
        self.alvaziGlDb.drop_table(self.config.get("gldbGlItemsTableName"))
        self.alvaziGlDb.create_gl_table(self.config.get("gldbGlItemsTableName"))

    def closeGldb(self):
        """
        Closes the database connection.
        """
        self.alvaziGlDb.close()

    def processBankTransactionCsvFiles(self):
        # Use BankCSVIterator to iterate over CSV files and print bank account codes and file paths
        bankCsvIterator = BankCSVIterator(self.config.get("bankFilesFolderPath"))
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
            gl_document = GLDocument(bank_transaction, bank_transactions.bank_account)
            print(f"--- Processing transaction {index} : {bank_transaction.Amount} {gl_document.items[1].currency_unit} / {gl_document.items[0].account_id} - {gl_document.items[1].account_id}")
            if not gl_document._gl_items_exist(self.alvaziGlDb):
                gl_document.insert_gl_items_into_db(self.alvaziGlDb)

# Main program:
# Create Main object, refresh the database, process the bank transaction CSV files, and close the database connection

if __name__ == "__main__":
    main = Main()
    main.refreshGlItemsTable()
    main.processBankTransactionCsvFiles()
    main.closeGldb()

