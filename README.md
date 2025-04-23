# MicroGL
Utilities to support a simple personal GL using exported bank transactions and Excel.

## Workflow
1) Download bank transactions in CSV format
2) Run the main program to:
- Import the CSV files.
- Create financial documents (Debit and Credit lines) and insert into an SQLite database.
- Read the documents and insert into an Excel workbook sheet table.

## Prerequisites
There are three configuration files in JSON format that drive the processing.
1) Constants.json
- Defines file paths, file names, database table name.
2) ChartOfAccounts.json
- Defines the chart of accounts and account properties.
3) BankAccounts.json
- Defines properties of each bank account. This includes structural information to parse the CSV file for each bank account and mapping to derive the GL accounts.