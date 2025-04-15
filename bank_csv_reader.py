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
    def __init__(self, bank_account_code: str, csv_file_path: str, bank_account: BankAccount):
        self.bank_account_code = bank_account_code
        self.csv_file_path = csv_file_path
        self.bank_account = bank_account
        self.bank_transaction_records = self._read_csv_file()
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

    def _read_csv_file(self):
        print(f"csvFileHasHeader: {self.bank_account.properties['csvFileHasHeader']}")
        print(f"csvFileColumnTitles: {self.bank_account.properties['csvFileColumnTitles']}")
        print(f"csvFileColumns: {self.bank_account.properties['csvFileColumns']}")
        if self.bank_account.properties["csvFileHasHeader"]:
            df = pd.read_csv(
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
            df = pd.read_csv(
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
        
        # Post-processing the DataFrame

        # 1) Standardize the Amount column to use a decimal point
        # This is necessary for German banks that use a comma as a decimal separator
        df['Amount'] = df['Amount'].apply(lambda x: str(x).replace(',', '.'))
        
        # 2) Convert the Amount column to Decimal type and round to 2 decimal places
        df['Amount'] = df['Amount'].apply(lambda x: round(Decimal(x), 2))
        
        # 3) Replace empty, NaN, or space-only values in the "Description" column with "<No Description>"
        df['Description'] = df['Description'].apply(
            lambda x: '<No Description>' if pd.isna(x) or str(x).strip() == '' else x
        )

        # Return the adjusted DataFrame
        return df


    def _filter_bank_records(self):
        filter_strings = self.bank_account.properties.get("bankRecordFilterStrings", [])
        if filter_strings:
            pattern = '|'.join(filter_strings)
            self.bank_transaction_records = self.bank_transaction_records[~self.bank_transaction_records['Description'].str.contains(pattern, na=False)]
