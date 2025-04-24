import os
import pandas as pd
from bank_account import BankAccount
from decimal import Decimal

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
            bank_account_code = file_name.split('-')[0]
            return bank_account_code, os.path.join(self.folder_path, file_name)
        else:
            raise StopIteration

class BankCSVReader:
    bank_account_code: str
    csv_file_path: str
    bank_account: BankAccount
    bank_transaction_records: pd.DataFrame

    def __init__(self, bank_account_code: str, csv_file_path: str, bank_account: BankAccount):
        self.bank_account_code = bank_account_code
        self.csv_file_path = csv_file_path
        self.bank_account = bank_account
        self.bank_transaction_records = self._read_csv_file()
        self._post_process_bank_transaction_records()
        self._filter_bank_records()

    def _derive_date_format(self, date_format_string: str) -> str:
        format_mappings = {
            "YYYY": "%Y",
            "YY": "%y",
            "MM": "%m",
            "DD": "%d"
        }
        for key, value in format_mappings.items():
            date_format_string = date_format_string.replace(key, value)
        return date_format_string

    def _read_csv_file(self) -> pd.DataFrame:
        print(f"csvFileHasHeader: {self.bank_account.properties['csvFileHasHeader']}")
        print(f"csvFileColumnTitles: {self.bank_account.properties['csvFileColumnTitles']}")
        print(f"csvFileColumns: {self.bank_account.properties['csvFileColumns']}")

        bank_transaction_records: pd.DataFrame

        if self.bank_account.properties["csvFileHasHeader"]:
            bank_transaction_records = pd.read_csv(
                self.csv_file_path,
                sep=self.bank_account.properties["csvFileSeparator"],
                usecols=self.bank_account.properties["csvFileColumns"],
                names=self.bank_account.properties["csvFileColumnTitles"],
                header=0,
                index_col=None,
                parse_dates=["Date"],
                date_format=self._derive_date_format(self.bank_account.properties["dateFormat"]),
                dtype={"CheckNo": str}  # Ensure CheckNo is read as a string
            )
        else:
            bank_transaction_records = pd.read_csv(
                self.csv_file_path,
                sep=self.bank_account.properties["csvFileSeparator"],
                usecols=self.bank_account.properties["csvFileColumns"],
                names=self.bank_account.properties["csvFileColumnTitles"],
                header=None,
                index_col=None,
                parse_dates=["Date"],
                date_format=self._derive_date_format(self.bank_account.properties["dateFormat"]),
                dtype={"CheckNo": str}  # Ensure CheckNo is read as a string
            )

        # Return the DataFrame
        return bank_transaction_records

    def _post_process_bank_transaction_records(self):

        # 1) Standardize the Amount column to use a decimal point
        # This is necessary for German banks that use a comma as a decimal separator
        self.bank_transaction_records['Amount'] = self.bank_transaction_records['Amount'].apply(lambda x: str(x).replace(',', '.'))
        
        # 2) Convert the Amount column to Decimal type and round to 2 decimal places
        self.bank_transaction_records['Amount'] = self.bank_transaction_records['Amount'].apply(lambda x: round(Decimal(x), 2))
        
        # 3) Replace empty, NaN, or space-only values in the "Description" column with "<No Description>"
        self.bank_transaction_records['Description'] = self.bank_transaction_records['Description'].apply(
            lambda x: '<No Description>' if pd.isna(x) or str(x).strip() == '' else x
        )

        # 4) Add a column for the bank CSV file name
        self.bank_transaction_records['CSVFile'] = os.path.basename(self.csv_file_path)

        # 5) Add a column for the row index
        self.bank_transaction_records['RowIndex'] = self.bank_transaction_records.index + 1  # Adding 1 to make it 1-based index

        # print the first 5 rows of the DataFrame for debugging
        print(self.bank_transaction_records.head())

    def _filter_bank_records(self):
        """
        Excludes bank transaction records based on the filter strings defined in the bank account properties.
        The filter strings are used to identify transactions that should be excluded from processing.
        The filter strings are defined in the bank account properties under the key "bankRecordFilterStrings".
        This is needed for Vanguard accounts to exclude transactions that are not relevant for the GL, e.g. sweep transactions.
        """
        filter_strings = self.bank_account.properties.get("bankRecordFilterStrings", [])
        if filter_strings:
            pattern = '|'.join(filter_strings)
            self.bank_transaction_records = self.bank_transaction_records[~self.bank_transaction_records['Description'].str.contains(pattern, na=False)]
