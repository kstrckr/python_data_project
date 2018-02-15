import sqlite3

class IowaLiquorDB:

    def __init__(self, db_name):
        self.db_name = db_name
        self.db = sqlite3.connect(self.db_name)

    store_table_cols = {
        'storeNumber': 'integer',
        'storeName': 'text',
        'address': 'text',
        'city': 'text',
        'zipCode': 'integer',
        'storeLattitude': 'real',
        'storeLongitude': 'real',
        'countyNumber': 'integer',
        'countyName': 'text',
    }


New_ILS_DB = IowaLiquorDB('sales.db')