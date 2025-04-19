import hashlib
import os
from decimal import Decimal
from gl_item import GLItem
from bank_account import BankAccount
from database import Database
from chart_of_accounts import ChartOfAccounts
from constants import Constants
import os

class GLDocument:
    def __init__(self, bank_transaction_record, bank_account: BankAccount, chart_of_accounts: ChartOfAccounts, constants: Constants):
        self.bank_transaction_record = bank_transaction_record
        self.bank_account = bank_account
        self.chart_of_accounts = chart_of_accounts
        self.constants = constants  # Store the constants
        self.items = []
        self.bank_transaction_category = self._determine_bank_transaction_category()
        self._assign_transaction_id()
        self._add_gl_item()
        self._add_offsetting_gl_item()

    def _determine_bank_transaction_category(self) -> str:
        if self.bank_account.properties["bankAccountType"] == self.constants.get('bankAccountTypes')['debit']:
            if self.bank_transaction_record.Amount >= 0:
                return self.constants.get('bankTransactionCategories')['deposit']
            else:
                return self.constants.get('bankTransactionCategories')['withdrawal']
        else:
            if self.bank_transaction_record.Amount >= 0:
                return self.constants.get('bankTransactionCategories')['withdrawal']
            else:
                return self.constants.get('bankTransactionCategories')['deposit']
        
    def _assign_transaction_id(self):
        """
        Assigns a unique transaction ID based on:
        - Date
        - Amount
        - Description
        - Bank account code
        The transaction ID is generated using SHA-256 hashing algorithm to ensure uniqueness.
        *** Open issue: it can happen that there are two transactions with the same date, amount, and description.
        In this case, the transaction ID will be the same. Need to find a solution for this.
        """
        self.transaction_id = hashlib.sha256(
            f"{self.bank_transaction_record.Date}{self.bank_transaction_record.Amount}\
                {self.bank_transaction_record.Description}{self.bank_account.properties['bankAccountCode']}".encode()
        ).hexdigest()

    def _add_gl_item(self):
        gl_mapping = self.bank_account.get_gl_mapping_for_search_string(
            self.bank_transaction_record.Description, 
            self.bank_transaction_category
        )
        # Determine the debit/credit indicator based on the bank transaction category for the P&L account
        # - For deposits, the bank account (balance sheet) is debited and the P&L account is credited.
        # - For withdrawals, the bank account (balance sheet) is credited and the P&L account is debited.
        if self.bank_transaction_category == self.constants.get('bankTransactionCategories')['deposit']:
            debit_credit_indicator = self.constants.get('dcIndicators')['credit']
        else:
            debit_credit_indicator = self.constants.get('dcIndicators')['debit']

        investment_name = getattr(self.bank_transaction_record, 'Investment', None)
        investment_symbol = getattr(self.bank_transaction_record, 'Symbol', None)
        check_no = getattr(self.bank_transaction_record, 'CheckNo', None)

        account_properties = self.chart_of_accounts.get_account_properties(gl_mapping["glAccount"])
        if not account_properties:
            raise ValueError(f"Account properties not found for GL account: {gl_mapping['glAccount']}")

        if debit_credit_indicator == self.constants.get('dcIndicators')['debit']:
            transaction_amount = abs(self.bank_transaction_record.Amount)
        else:
            transaction_amount = -abs(self.bank_transaction_record.Amount)

        gl_item = GLItem(
            transaction_id=self.transaction_id,
            transaction_item_id="001",
            bank_csv_file="", #os.path.basename(self.bank_transaction_record.CSVFile),
            bank_csv_row_index="", #self.bank_transaction_record.RowIndex,
            transaction_date=self.bank_transaction_record.Date.to_pydatetime(),
            posting_year=self.bank_transaction_record.Date.year,
            posting_period=self.bank_transaction_record.Date.month,
            transaction_amount=transaction_amount,
            currency_unit=self.bank_account.properties["currencyUnit"],
            debit_credit_indicator=debit_credit_indicator,
            transaction_description=self.bank_transaction_record.Description,
            account_id=gl_mapping["glAccount"],
            business_partner=gl_mapping["bp"],
            bank_account_code=self.bank_account.properties["bankAccountCode"],
            investment_name=investment_name,
            investment_symbol=investment_symbol,
            check_no=check_no,
            account_type=account_properties["accountType"],
            is_taxable=account_properties["isTaxable"]
        )
        self.items.append(gl_item)

    def _add_offsetting_gl_item(self):
        offsetting_transaction_item_id = "002"
        offsetting_transaction_amount = -self.items[0].transaction_amount
        offsetting_debit_credit_indicator = (
            "C" if self.items[0].debit_credit_indicator == "D" else "D"
        )
        offsetting_account_id = self.bank_account.properties["balanceSheetAccount"]

        account_properties = self.chart_of_accounts.get_account_properties(offsetting_account_id)

        offsetting_gl_item = GLItem(
            transaction_id=self.transaction_id,
            transaction_item_id=offsetting_transaction_item_id,
            bank_csv_file=self.items[0].bank_csv_file,
            bank_csv_row_index=self.items[0].bank_csv_row_index,
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
            investment_name=self.items[0].investment_name,
            investment_symbol=self.items[0].investment_symbol,
            check_no=self.items[0].check_no,
            account_type=account_properties["accountType"],
            is_taxable=account_properties["isTaxable"]
        )
        self.items.append(offsetting_gl_item)

    def insert_gl_items_into_db(self, glDb: Database):
        """
        Inserts General Ledger (GL) items into the database.

        This method calculates the total amount of all GL items and ensures that the total is zero.
        It then inserts each GL item into the `gl_items` table in the database.

        Args:
            glDb (Database): The database connection object.

        Raises:
            AssertionError: If the total amount of GL items is not zero.

        """
        total_amount = sum(item.transaction_amount for item in self.items)
        assert total_amount == 0, f"Total GL item amounts must be zero, but got {total_amount}"

        for item in self.items:
            glDb.cursor.execute(
            f"""
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
                bank_account_code,
                investment_name,
                investment_symbol,
                check_no,
                account_type,
                is_taxable
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.transaction_id,
                item.transaction_item_id,
                item.transaction_date,
                item.posting_year,
                item.posting_period,
                item.transaction_amount,
                item.currency_unit,
                item.debit_credit_indicator,
                item.transaction_description,
                item.account_id,
                item.business_partner,
                item.bank_account_code,
                item.investment_name,
                item.investment_symbol,
                item.check_no,
                item.account_type,
                item.is_taxable
            ),
            )
        glDb.commit()

    def _gl_items_exist(self, glDb: Database) -> bool:
        glDb.cursor.execute(
            f"""
            SELECT COUNT(*) FROM gl_items WHERE transaction_id = ?
        """,
            (self.transaction_id,),
        )
        count = glDb.cursor.fetchone()[0]
        return count > 0
