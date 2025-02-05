import hashlib
from decimal import Decimal
from gl_item import GLItem
from bank_account import BankAccount
from database import Database

class GLDocument:
    def __init__(self, bank_transaction_record, bank_account: BankAccount):
        self.bank_transaction_record = bank_transaction_record
        self.bank_account = bank_account
        self.items = []
        self.bank_transaction_category = self._determine_bank_transaction_category()
        self._assign_transaction_id()
        self._add_gl_item()
        self._add_offsetting_gl_item()

    def _determine_bank_transaction_category(self) -> str:
        if self.bank_account.properties["bankAccountType"] == "Debit":
            if self.bank_transaction_record.Amount >= 0:
                return "D"
            else:
                return "C"
        else:
            if self.bank_transaction_record.Amount >= 0:
                return "C"
            else:
                return "D"
        
    def _assign_transaction_id(self):
        self.transaction_id = hashlib.sha256(
            f"{self.bank_transaction_record.Date}{self.bank_transaction_record.Amount}\
                {self.bank_transaction_record.Description}{self.bank_account.properties['bankAccountCode']}".encode()
        ).hexdigest()

    def _add_gl_item(self):
        gl_mapping = self.bank_account.get_gl_mapping_for_search_string(
            self.bank_transaction_record.Description, 
            self.bank_transaction_category
        )
               
        if self.bank_transaction_record.Amount >= 0:
            debit_credit_indicator = "C"
        else:
            debit_credit_indicator = "D"

        investment_name = getattr(self.bank_transaction_record, 'Investment', None)
        investment_symbol = getattr(self.bank_transaction_record, 'Symbol', None)
        check_no = getattr(self.bank_transaction_record, 'CheckNo', None)

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
            investment_name=investment_name,
            investment_symbol=investment_symbol,
            check_no=check_no
        )
        self.items.append(gl_item)

    def _add_offsetting_gl_item(self):
        offsetting_transaction_item_id = "002"
        offsetting_transaction_amount = -self.items[0].transaction_amount
        offsetting_debit_credit_indicator = (
            "C" if self.items[0].debit_credit_indicator == "D" else "D"
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
            investment_name=self.items[0].investment_name,
            investment_symbol=self.items[0].investment_symbol,
            check_no=self.items[0].check_no
        )
        self.items.append(offsetting_gl_item)

    def insert_gl_items_into_db(self, glDb: Database):
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
                    check_no
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    item.check_no
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
