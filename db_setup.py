import sqlite3

def db_connect(db_name):
        con = sqlite3.connect(db_name)
        return con

class StoreSchema:

    store_table_cols = '''
        store_number INTEGER PRIMARY KEY, 
        store_name TEXT NOT NULL, 
        address TEXT, 
        city TEXT,
        zip_code INTEGER, 
        store_lat REAL, 
        store_long REAL, 
        county_number INTEGER, 
        county_name TEXT'''
            
    table_statement = 'CREATE TABLE IF NOT EXISTS stores'

    def create_stores_table(self):
        full_statement = self.table_statement + '(' + self.store_table_cols + ')'
        return full_statement

if __name__ == "__main__":
    New_ILS_DB = db_connect('sales.db')
    store_schema = StoreSchema()
    New_ILS_DB.execute(store_schema.create_stores_table())

'''INSERT INTO stores VALUES (2191, 'Keokuk Spirits', '1013 MAIN', 'KEOKUK', 52632, 40.29978, -91.387531, 56, 'LEE')'''