"""
Alvazi micro GL processor

This program processes bank transaction CSV files, records the transactions in a General Ledger (GL) database,
and exports the GL items to an Excel sheet. It uses various modules to handle bank accounts, database operations,
GL items, and document processing.

Modules:
- gl_processor: For processing GL items and handling the main program logic.
"""

# Libraries
from gl_processor import GLProcessor  

# Main program:
# Create GLProcessor object, refresh the database, process the bank transaction CSV files, and close the database connection

if __name__ == "__main__":
    processor = GLProcessor()
    processor.refresh_gl_items_table()
    processor.process_bank_transaction_csv_files()
    processor.close_gldb()
    processor.write_gl_items_to_excel()

