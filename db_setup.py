import sqlite3

class IowaLiquorDB():

    def __init__(self, db_name):
        self.db_name = db_name
        self.db = sqlite3.connect(self.db_name)


class IowaLiquorStoresTable:
    
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

    store_table_list = [
        ['store_number', 'integer', 'UNIQUE'],
        ['store_name', 'text', 'NOT NULL'],
        ['address', 'text', 'NOT NULL'],
        ['city', 'text', 'NOT NULL'],
        ['zip_code', 'integer', 'NOT NULL'],
        ['store_lat', 'real', 'NOT NULL'],
        ['store_long', 'real', 'NOT NULL'],
        ['county_number', 'integer', 'NOT NULL'],
        ['county_name', 'text', 'NOT NULL'],
    ]

    def parse_col_str(self, str_list):
        table_parameters = ''
        for row in str_list:
            line = '''{} {} {}, 
            '''.format(row[0], row[1], row[2])
            table_parameters += line
        return table_parameters

    def create_table(self):
        
        return '''CREATE TABLE stores (
            {})'''.format(self.parse_col_str(self.store_table_list).strip(',\n '))


New_ILS_DB = IowaLiquorDB('sales.db')
Store_Table = IowaLiquorStoresTable()
store_table_str = Store_Table.create_table()
print(store_table_str)
New_ILS_DB.db.execute(store_table_str)