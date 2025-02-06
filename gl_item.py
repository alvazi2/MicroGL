from dataclasses import dataclass
from decimal import Decimal

@dataclass
class GLItem:
    transaction_id: str
    transaction_item_id: int
    transaction_date: str
    posting_year: int
    posting_period: int
    transaction_amount: Decimal
    currency_unit: str
    debit_credit_indicator: str
    transaction_description: str
    account_id: str
    business_partner: str
    bank_account_code: str
    investment_name: str
    investment_symbol: str
    check_no: str
    account_type: str
    is_taxable: bool
