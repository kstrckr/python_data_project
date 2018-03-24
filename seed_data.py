
import csv
import datetime
import time
import os
import re
import sqlite3

import db_setup as setup

#-- CSV READERS
def read_csv_generator(filename):
    '''a generator function that reads through the CSV file,
    yielding a single row as needed'''
    print('Generator Initiated')
    with open(filename, 'r') as raw_data:
        
        data_reader = csv.reader(raw_data)

        next(data_reader)
        # counter = 0
        for row in data_reader:
            # if row[1][6:] == 2016:
            # counter += 1
            yield row
            # else:
            #     continue
            # if counter == 9000000:
            #     break
    print('\nGenerator Complete')

#-- FIELD FORMATTING
def format_date_field(field):
    if field:
        output_date_str = datetime.datetime.strptime(field, '%m/%d/%Y').strftime('%Y-%m-%d')
        return output_date_str
    else:
        return None

def format_int_field(field):
    if field:
        return int(field)
    else:
        return None

def format_lat_long(field):
    '''this function takes an entry. It regex matches lattitude and longitude values
    inside parens and comma seperated and returns a tuple for lat, long'''

    lat, long = (None, None)

    if field:
        pattern = re.compile(r'^.*\((?P<lat>-?\d+\.\d+), (?P<long>-?\d+\.\d+).*$')
        match = pattern.match(field.replace('\n', ' '))
        try:
            lat = float(match.group('lat'))
            long = float(match.group('long'))
        except:
            pass
    return (lat, long)

def format_liter_to_ml(field):
    if field:
        field = float(field) * 1000
        ml = int(field)
        return ml
    else:
        return None

def format_money_field(field):

    if field:
        if field[0] == '$':
            pennies = field[1:].replace('.', '')
        else:
            pennies = field.replace('.', '')
        return int(pennies)
    else:
        return None

def format_text_field(field):
    '''Cleans up txt fields (address, store name, city) to each word capitalized and striped of apostraphies'''
    if field:
        each_word_upper_str = ' '.join([word.capitalize() for word in field.strip().split(' ')])
        return each_word_upper_str
    else:
        return None

def format_zip_code(field):
    if len(field) == 5:
        try:
            field = int(field)
            return field
        except ValueError:
            pass
    else:
        return None

#-- ROW PARSING
def parse_a_row(row):
    '''parse an entire row from the raw csv'''
    #----   sale fields (1)
    row[0] = format_text_field(row[0])      # invoice num
    row[1] = format_date_field(row[1])      # date
    #----   store fields
    row[2] = format_int_field(row[2])       # store number
    row[3] = format_text_field(row[3])      # store name
    row[4] = format_text_field(row[4])      # address
    row[5] = format_text_field(row[5])      # city
    row[6] = format_zip_code(row[6])        # zip code
    row[7] = format_text_field(row[7])      # lat and long
    lat, long = format_lat_long(row[7]) 
    row[7] = lat                            # lat
    row.insert(8, long)                     # long
    row[9] = format_int_field(row[9])       # county number
                                            # county name is pre-seeded during table creation
    #----   cateogry fields
    row[11] = format_int_field(row[11])     # category number
    row[12] = format_text_field(row[12])    # category name
    #----   vendor fields
    row[13] = format_int_field(row[13])     # vendor number
    row[14] = format_text_field(row[14])    # vendor name
    #----   item fields
    row[15] = format_int_field(row[15])     # item number
    row[16] = format_text_field(row[16])    # item description
    row[17] = format_int_field(row[17])     # pack qty
    row[18] = format_int_field(row[18])     # bottle volume ml
    row[19] = format_money_field(row[19])   # state wholesale cost
    row[20] = format_money_field(row[20])   # state retail cost
    #----   sale fields (2)
    row[21] = format_int_field(row[21])     # quantity sold
    row[22] = format_money_field(row[22])   # sale ammount
    row[23] = format_liter_to_ml(row[23])   # sale volume in ml

    return row

def parse_a_selective_row(row, categories, items, sales, stores, vendors):
    '''parse an entire row from the raw csv'''
    
    row.insert(8, None)

    temp_sales = []
    temp_stores = []
    temp_categories = []
    temp_vendors = []
    temp_items = []

    #----   sale fields (1)
    if not row[0] in sales:
        row[0] = format_text_field(row[0])      # invoice num
        row[1] = format_date_field(row[1])      # date

        row[2] = format_int_field(row[2])       # store number FK
        row[15] = format_int_field(row[15])     # item number FK
        #----   sale fields (2)
        row[21] = format_int_field(row[21])     # quantity sold
        row[22] = format_money_field(row[22])   # sale ammount
        row[23] = format_liter_to_ml(row[23])   # sale volume in ml

        temp_sales = [row[0], (row[0], row[1], row[2], row[15], row[21], row[22], row[23])]

    #----   store fields
    if not row[2] in stores:
        # row[2] = format_int_field(row[2])       # store number
        row[3] = format_text_field(row[3])      # store name
        row[4] = format_text_field(row[4])      # address
        row[5] = format_text_field(row[5])      # city
        row[6] = format_zip_code(row[6])        # zip code
        row[7] = format_text_field(row[7])      # lat and long
        lat, long = format_lat_long(row[7]) 
        row[7] = lat                            # lat
        row[8] = long                           # long
        row[9] = format_int_field(row[9])       # county number

        temp_stores = [row[2], (row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])]
    #----   cateogry fields
    if not row[11] in categories:
        row[11] = format_int_field(row[11])     # category number
        row[12] = format_text_field(row[12])    # category name

        temp_categories = [row[11], (row[11], row[12])]
    #----   vendor fields
    if not row[13] in vendors:
        row[13] = format_int_field(row[13])     # vendor number
        row[14] = format_text_field(row[14])    # vendor name

        temp_vendors = [row[13], (row[13], row[14])]
    #----   item fields
    if not row[15] in items:
        # row[15] = format_int_field(row[15])     # item number
        row[11] = format_int_field(row[11])     # category number FK
        row[13] = format_int_field(row[13])     # vendor number FK
        row[16] = format_text_field(row[16])    # item description
        row[17] = format_int_field(row[17])     # pack qty
        row[18] = format_int_field(row[18])     # bottle volume ml
        row[19] = format_money_field(row[19])   # state wholesale cost
        row[20] = format_money_field(row[20])   # state retail cost

        temp_items = [row[15], (row[15], row[11], row[13], row[16], row[17], row[18], row[19], row[20])]

    return (temp_sales, temp_stores, temp_categories, temp_vendors, temp_items)

def create_all_tables(db):

    print('Creating Tables')

    with setup.db_connect(db) as database:

        county_table = setup.CountySchema()
        database.execute(county_table.create_counties_table())
        county_table.insert_counties(database)

        stores_table = setup.StoreSchema()
        database.execute(stores_table.create_stores_table())

        categories_table = setup.CategorieSchema()
        database.execute(categories_table.create_categories_table())

        vendors_table = setup.VendorSchema()
        database.execute(vendors_table.create_vendors_table())

        items_table = setup.ItemSchema()
        database.execute(items_table.create_items_table())

        sales_table = setup.SaleSchema()
        database.execute(sales_table.create_sales_table())

def build_virtual_db(raw_row_generator):
    categories = {}
    items = {}
    sales = {}
    stores = {}
    vendors = {}
    # bytes_read = 0

    # count = 0
    for row in raw_row_generator:
        # count += 1

        new_sales, new_stores, new_categories, new_vendors, new_items = parse_a_selective_row(row, categories, items, sales, stores, vendors)

        if new_sales:
            sales[new_sales[0]] = new_sales[1]
        if new_stores:
            stores[new_stores[0]] = new_stores[1]
        if new_categories:
            categories[new_categories[0]] = new_categories[1]
        if new_vendors:
            vendors[new_vendors[0]] = new_vendors[1]
        if new_items:
            items[new_items[0]] = new_items[1]

        # if count == 1000000:
        #     break
    return (categories, items, sales, stores, vendors)

#-- TABLE INSERTS

def insert_sales(db, dict_of_sales):
    
    insert_statement = '''INSERT INTO sales(sale_id, sale_date, store_id, item_id, bottles_sold, sale_value, sale_vol_ml)
                        VALUES(?, ?, ?, ?, ?, ?, ?)'''

    
    with setup.db_connect(db) as database:
        
        cur = database.cursor()
        cur.executemany(insert_statement, dict_of_sales.values())

def insert_stores(db, dict_of_stores):

    insert_statment = '''INSERT INTO stores(store_id, store_name, address, city, zip_code, store_lat, store_long, county_id)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)'''

    with setup.db_connect(db) as database:
        cur = database.cursor()
        cur.executemany(insert_statment, dict_of_stores.values())

def insert_categories(db, dict_of_categories):

    insert_statement = '''INSERT INTO categories (category_id, category_name)
                        VALUES(?, ?)'''

    with setup.db_connect(db) as database:
        cur = database.cursor()
        cur.executemany(insert_statement, dict_of_categories.values())

def insert_vendors(db, dict_of_vendors):

    insert_statement = '''INSERT INTO vendors (vendor_id, vendor_name)
                        VALUES(?, ?)'''

    with setup.db_connect(db) as database:
        cur = database.cursor()
        cur.executemany(insert_statement, dict_of_vendors.values())

def insert_items(db, dict_of_items):

    insert_statement = '''INSERT INTO items (item_id, category_id, vendor_id, item_description, pack_qty, bottle_volume_ml, state_wholesale, state_retail)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)'''

    with setup.db_connect(db) as database:
        cur = database.cursor()
        cur.executemany(insert_statement, dict_of_items.values())

if __name__ == '__main__':

    # sets start time for efficiency comparisons
    start = time.time()

    # name of target DB to be created or connected to in the directory set in the following step
    db_name = 'sales_new_seed.db'
    # path to the target db
    target_db = os.path.join('output', db_name)

    # creates all tables required for the project IF NOT EXISTS
    create_all_tables(target_db)

    # path to the input CSV
    rel_path_to_data = os.path.join('input', 'iowa-liquor-sales', 'Iowa_Liquor_Sales.csv')

    # CSV reader creates a generator for line-by-line parsing of the input data
    raw_data_generator = read_csv_generator(rel_path_to_data)

    print('Parsing data')

    # builds a dictionary for each table, with a value eaual to each row's PK and a value of a tuple for each row of parsed data
    categories, items, sales, stores, vendors = build_virtual_db(raw_data_generator)
    print('Parsing complete')

    # parsing results
    print('\nReady to insert:\n{} Categories\n{} items\n{} sales\n{} stores\n{} vendors\n'.format(len(categories), len(items), len(sales), len(stores), len(vendors)))


    # inserts to each table in turn
    print('inserting sales')
    insert_sales(target_db, sales)
    print('inserting stores')
    insert_stores(target_db, stores)
    print('inserting categories')
    insert_categories(target_db, categories)
    print('inserting vendors')
    insert_vendors(target_db, vendors)
    print('inserting items')
    insert_items(target_db, items)

    stop = time.time()
    # final results
    print('\ndatabase seeding took {} seconds'.format(stop - start))
    
