import json

class ChartOfAccounts:
    """
    A class to represent a chart of accounts.

    Attributes:
    json_file_path (str): The path to the JSON file containing the chart of accounts.
    accounts (dict): A dictionary of accounts with account IDs as keys.
    """
    def __init__(self, json_file_path: str):
        """
        Constructs all the necessary attributes for the ChartOfAccounts object.

        Parameters:
        json_file_path (str): The path to the JSON file containing the chart of accounts.
        """
        self.json_file_path = json_file_path
        self.accounts = self._read_chart_of_accounts()

    def _read_chart_of_accounts(self):
        """
        Reads the chart of accounts from a JSON file.

        Returns:
        dict: A dictionary of accounts with account IDs as keys.
        """
        with open(self.json_file_path, "r") as file:
            return {account["accountId"]: account for account in json.load(file)["chartOfAccounts"]}

    def get_account_properties(self, account_id: str) -> dict:
        """
        Gets the properties of an account by its ID.

        Parameters:
        account_id (str): The ID of the account.

        Returns:
        dict: A dictionary of account properties.
        """
        return self.accounts.get(account_id, {})
