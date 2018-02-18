import sqlite3

class IowaLiquorDB():

    def __init__(self, db_name):
        self.db_name = db_name
        self.db = sqlite3.connect(self.db_name)


class IowaLiquorStoresTable:

    store_table_list = [
        ['store_number', 'INTEGER', 'PRIMARY KEY'],
        ['store_name', 'TEXT', 'NOT NULL'],
        ['address', 'TEXT'],
        ['city', 'TEXT'],
        ['zip_code', 'INTEGER'],
        ['store_lat', 'REAL'],
        ['store_long', 'REAL'],
        ['county_number', 'INTEGER'],
        ['county_name', 'TEXT'],
    ]

    def parse_col_str(self, str_list):
        table_parameters = ''
        for row in str_list:
            try:
                line = '''{} {} {}, 
                '''.format(row[0], row[1], row[2])
            except IndexError:
                 line = '''{} {}, 
                '''.format(row[0], row[1])               
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