import sqlite3

class IowaLiquorDB():

    def __init__(self, db_name):
        self.db_name = db_name
        self.db = sqlite3.connect(self.db_name)


class IowaLiquorStoresTable:

    store_table_list = [
        ['store_number', 'INTEGER', 'PRIMARY KEY'],
        ['store_name', 'TEXT', 'NOT NULL'],
        ['address', 'TEXT', 'NOT NULL'],
        ['city', 'TEXT', 'NOT NULL'],
        ['zip_code', 'INTEGER', 'NOT NULL'],
        ['store_lat', 'REAL', 'NOT NULL'],
        ['store_long', 'REAL', 'NOT NULL'],
        ['county_number', 'INTEGER', 'NOT NULL'],
        ['county_name', 'TEXT', 'NOT NULL'],
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

if __name__ == "__main__":
    New_ILS_DB = IowaLiquorDB('sales.db')
    Store_Table = IowaLiquorStoresTable()
    store_table_str = Store_Table.create_table()
    print(store_table_str)
    New_ILS_DB.db.execute(store_table_str)

'''INSERT INTO stores VALUES (2191, 'Keokuk Spirits', '1013 MAIN', 'KEOKUK', 52632, 40.29978, -91.387531, 56, 'LEE')'''