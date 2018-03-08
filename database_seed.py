import csv
import datetime
import time
import os
import re
import sqlite3

import db_setup as setup

clear = lambda: os.system('cls')

def build_path(filename):
    '''lets you use a relative path to the data, rather than requiring absolute'''
    script_dir = os.path.dirname(__file__)
    rel_path = filename
    abs_file_path = os.path.join(script_dir, rel_path)

    return abs_file_path

def read_csv(filename):
    '''a generator function that reads through the CSV file,
    yielding a single row as needed'''
    print('Generator Initiated')
    with open(filename, 'r') as raw_data:
        data_reader = csv.reader(raw_data)

        next(data_reader)
        # counter = 0
        for row in data_reader:
            # if row[1][6:] == 2016:
            yield row
            # else:
            #     continue
    print('Generator Complete')

def parse_lat_long(data):
    '''this function takes an entry. It regex matches lattitude and longitude values
    inside parens and comma seperated and returns a tuple for lat, long'''
    pattern = re.compile(r'^.*\((?P<lat>-?\d+\.\d+), (?P<long>-?\d+\.\d+).*$')
    try:
        match = pattern.match(data)
        lat, long = match.groups()
    except AttributeError:
        null_string = ''
        lat = null_string
        long = null_string
    
    return (lat, long)

def format_text_field(data):
    '''Cleans up txt fields (address, store name, city) to each word capitalized and striped of apostraphies'''
    each_word_upper_str = ' '.join([word.capitalize() for word in data.strip().split(' ')])
    return each_word_upper_str

def format_money_field(data):

    if data and type(data) == str:
        
        if data[0] == '$':
            pennies = data[1:].replace('.', '')
        else:
            pennies = data.replace('.', '')
        
        return int(pennies)

    else:
        return data

def batch_stores_output(data_input):
    '''the for loop in this function checks against some condition, in this case Store Number
    and adds the store details to a dictionary (key = Store Number)'''
    output_complete = {}
    output_incomplete = {}
 
    for row in data_input:
        store_flagged = False
        # row = line[:10]

        row[3] = format_text_field(row[3])
        row[5] = format_text_field(row[5])

        lat, long = parse_lat_long(row[7].replace('\n', ' '))
        row[7] = lat
        row.insert(8, long)

        if not row[2] in output_complete:

            for i, data in enumerate(row):
                row[i] = row[i].replace('"', '')
                if not data:
                    row[i] = 'NULL'
                    if not store_flagged:
                        store_flagged = True

            if store_flagged:
                
                if row[2] in output_incomplete:
                    updated = False
                    if output_incomplete[row[2]] != row[2:11]:
                        for i, data in enumerate(row[2:11]):
                            if output_incomplete[row[2]][i] == 'NULL' and row[i+2] != 'NULL':
                                output_incomplete[row[2]][i] = row[i+2]
                                updated = True
                        if updated:
                            # print('updated row = ', output_incomplete[row[2]])
                            updated = False
                else:
                    output_incomplete[row[2]] = row[2:11]
                    # print('store_flagged row = ', output_incomplete[row[2]])

            else:
                output_complete[row[2]] = row[2:11]
                # print('parsed row = ', output_complete[row[2]])

    for key, value in output_incomplete.items():
        if not key in output_complete:
            output_complete[key] = value
    
    return output_complete

def batch_category_output(data_input):
    pass

def insert_stores(list_of_stores, database):

    for store, data in list_of_stores.items():

        row = {
            'store_number': data[0],
            'store_name': data[1],
            'address': data[2],
            'city': data[3],
            'zip_code': data[4],
            'store_lat': data[5],
            'store_long': data[6],
            'county_number': data[7],
            # 'county_name': data[8],
        }

        if not row['county_number'] or row['county_number'] == 'NULL':
            row['county_number'] = 0

        values = '{}, "{}", "{}", "{}", {}, {}, {}, {}'.format(
                                row['store_number'],
                                row['store_name'],
                                row['address'],
                                row['city'],
                                row['zip_code'],
                                row['store_lat'],
                                row['store_long'],
                                row['county_number'])

        properly_nulled_values = values.replace(r'"NULL"', 'NULL')

        command = '''INSERT INTO stores VALUES ({})'''.format(properly_nulled_values)

        print(command)

        database.execute(command)
        # print('Inserted {} into database'.format(row['store_name']))

    database.commit()
    print('Inserts Committed')

def seed_counties():
    
    with setup.db_connect('sales_db.db') as database:

        county_table = setup.CountySchema()
        database.execute(county_table.create_counties_table())
        county_table.insert_counties(database)

def create_all_tables(db_name):
    with setup.db_connect(db_name) as database:

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

def seed_unique_stores(db_name):

    abs_path_of_source_data = build_path(r'iowa-liquor-sales\Iowa_Liquor_Sales.csv')
    raw_data_generator = read_csv(abs_path_of_source_data)
    all_unique_stores = batch_stores_output(raw_data_generator)

    with setup.db_connect(db_name) as database:
        insert_stores(all_unique_stores, database)

def parse_a_row(row):
    output_row = list(row)
    output_row[3] = format_text_field(row[3])
    output_row[5] = format_text_field(row[5])
    output_row[11] = format_text_field(row[11])
    output_row[13] = format_text_field(row[13])
    output_row[15] = format_text_field(row[15])
    output_row[18] = format_money_field(row[18])
    output_row[19] = format_money_field(row[19])
    output_row[21] = format_money_field(row[21])

    return output_row

def parse_a_sale(row):

    if not row[2]:
        row[2] = 0
    if not row[8]:
        row[8] = 0
    if not row[12]:
        row[12] = 0
    if not row[14]:
        row[14] = 0

    individual_sale = {
        'item_inv': row[0],
        'sale_date': datetime.datetime.strptime(row[1], '%m/%d/%Y').strftime('%Y-%m-%d'),
        'store_number': int(row[2]),
        'category_number': int(row[8]),
        'vendor_number': int(row[12]),
        'item_number': int(row[14]),
        'bottles_sold': int(row[20]),
        'sale_dollars': format_money_field(row[21]),
        'volume_sold_ml': int(float(row[22])*1000)
    }

    # sale_row = [
    #     individual_sale['item_inv'],
    #     individual_sale['sale_date'],
    #     individual_sale['store_number'],
    #     individual_sale['category_number'],
    #     individual_sale['vendor_number'],
    #     individual_sale['item_number'],
    #     individual_sale['bottles_sold'],
    #     individual_sale['sale_dollars'],
    #     individual_sale['volume_sold_ml'],
    # ]

    return individual_sale

def parse_a_store(row):
    row[3] = format_text_field(row[3])
    row[5] = format_text_field(row[5])

    lat, long = parse_lat_long(row[7].replace('\n', ' '))
    row[7] = lat
    row.insert(8, long)

    return row

def sort_a_store(row, complete_dicts, incomplete_dicts):
    store_flagged = False

    if not row[2] in incomplete_dicts:

        for i, data in enumerate(row):
            if type(row[i]) == str:
                row[i] = row[i].replace('"', '')
            if not data:
                row[i] = 'NULL'
                if not store_flagged:
                    store_flagged = True

        if store_flagged:
            
            if row[2] in incomplete_dicts:
                updated = False
                if incomplete_dicts[row[2]] != row[2:11]:
                    for i, data in enumerate(row[2:11]):
                        if incomplete_dicts[row[2]][i] == 'NULL' and row[i+2] != 'NULL':
                            incomplete_dicts[row[2]][i] = row[i+2]
                            updated = True
                    if updated:
                        print('updated row = ', incomplete_dicts[row[2]])
                        updated = False
            else:
                incomplete_dicts[row[2]] = row[2:11]
                print('store_flagged row = ', incomplete_dicts[row[2]])

        else:
            incomplete_dicts[row[2]] = row[2:11]
            print('parsed row = ', complete_dicts[row[2]])



def parse_seed_data(db_name, data_input):
    
    store_data_complete = {}
    store_data_incomplete = {}

    categories = {}
    vendors = {}
    items = {}

    sales = {}

    for unparsed_row in data_input:

        if unparsed_row[10] and not unparsed_row[10] in categories:
            row = parse_a_row(unparsed_row)
            categories[row[10]] = row[10:12]

        if unparsed_row[12] and not unparsed_row[12] in vendors:
            row = parse_a_row(unparsed_row)
            vendors[row[12]] = row[12:14]

        if unparsed_row[14] and not unparsed_row[14] in items:
            row = parse_a_row(unparsed_row)
            items[row[14]] = row[14:20]

        if unparsed_row[0] and not unparsed_row[0] in sales:
            row = parse_a_sale(unparsed_row)
            sales[unparsed_row[0]] = row
        else:
            print('DUPLICATE INVOICE NUMBER')

        if unparsed_row[2]:
            row = parse_a_store(unparsed_row)
            sort_a_store(row, store_data_complete, store_data_incomplete)

    for key, value in store_data_incomplete.items():
        if not key in store_data_incomplete:
            store_data_complete[key] = value

    print('''categories = {}\nvendors = {}\nitems = {}\nsales = {}\nstores = {}'''.format(len(categories), len(vendors), len(items), len(sales), len(store_data_complete)))

    return categories, vendors, items, sales, store_data_complete


        

    

if __name__ == "__main__":
    # seed_single_store()
    start = time.time()
    print(start)
    target_db = 'sales_db.db'
    # create_db(target_db)
    create_all_tables(target_db)

    abs_path_of_source_data = build_path(r'\input\iowa-liquor-sales\Iowa_Liquor_Sales.csv')
    raw_data_generator = read_csv(abs_path_of_source_data)

    categories = parse_seed_data(target_db, raw_data_generator)
    end = time.time()

    print(end - start)


    # seed_unique_stores(target_db)
