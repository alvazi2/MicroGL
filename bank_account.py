import json

class BankAccount:
    def __init__(self, property_file_path: str, bank_account_code: str):
        self.json_file_path = property_file_path
        self.bank_account_code = bank_account_code
        self.properties = self._read_bank_account_properties()

    def _read_bank_account_properties(self):
        with open(self.json_file_path, "r") as file:
            for bank_account_properties in json.load(file)["bankAccountProperties"]:
                if bank_account_properties["bankAccountCode"] == self.bank_account_code:
                    return bank_account_properties
        raise ValueError(f"No properties found for bank account code: {self.bank_account_code}")

    def get_gl_mapping_for_search_string(self, search_string: str, bank_transaction_category: str) -> dict:
        for gl_mapping in self.properties["glMapping"]:
            # print(f"Checking GL mapping: {gl_mapping['searchString']} against search string: {search_string}")
            if gl_mapping["searchString"] in search_string:
                return gl_mapping
        
        # If no mapping found, return the default mapping
        if bank_transaction_category == "D":
            return {
                "searchString": search_string,
                "glAccount": self.properties["missingGlMappingDefault"]["glAccountRevenue"],
                "bp": self.properties["missingGlMappingDefault"]["unknownBp"]
            }
        else:
            return {
                "searchString": search_string,
                "glAccount": self.properties["missingGlMappingDefault"]["glAccountExpense"],
                "bp": self.properties["missingGlMappingDefault"]["unknownBp"]
            }
