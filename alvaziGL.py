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
        return self.config.get(key)

# Load configuration
config = Config('constants.json')

# Main program logic
class Main:
    gldb_file_path: str
    bank_account_properties_file_path: str
    alvaziGl_db: Database

    def __init__(self, gldb_file_path: str, bank_account_properties_file_path: str):
        self.gldb_file_path = gldb_file_path
        self.bank_account_properties_file_path = bank_account_properties_file_path

        # Initialize the database (connection object)
        self.alvaziGl_db = Database(config.get("gldbFilePath"))

    def refresh_db(self):
        """
        Drops the GL items table and recreates it.
        """
        self.alvaziGl_db.drop_table(config.get("gldbGlItemsTableName"))
        self.alvaziGl_db.create_gl_table(config.get("gldbGlItemsTableName"))

    def close(self):
        """
        Closes the database connection.
        """
        self.alvaziGl_db.close()

    def process_bank_transaction_csv_files(self):
        # Use BankCSVIterator to iterate over CSV files and print bank account codes and file paths
        bank_csv_iterator = BankCSVIterator(config.get("bankFilesFolderPath"))
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
            print(f"--- Processing transaction {index} : {bank_transaction.Amount} {gl_document.items[1].currency_unit} / {gl_document.items[0].account_id} - {gl_document.items[1].account_id}")
            if not gl_document._gl_items_exist(self.alvaziGl_db):
                gl_document.insert_gl_items_into_db(self.alvaziGl_db)

# Main program:
# Create Main object, refresh the database, process the bank transaction CSV files, and close the database connection

if __name__ == "__main__":
    main_program = Main(config.get("gldbFilePath"), config.get("bankAccountPropertiesFilePath"))
    main_program.refresh_db()
    main_program.process_bank_transaction_csv_files()
    main_program.close()

