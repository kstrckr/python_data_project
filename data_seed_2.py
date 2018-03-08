
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
    
    if field:
        pattern = re.compile(r'^.*\((?P<lat>-?\d+\.\d+), (?P<long>-?\d+\.\d+).*$')
        match = pattern.match(field)
        lat, long = match.groups()
    else:
        lat, long = (None, None)

    return (lat, long)

def format_liter_to_ml(field):
    if field:
        ml = int(float(field)*1000)
        return ml
    else:
        return None

def format_money_field(field):

    if type(field) == str:
        if field[0] == '$':
            pennies = field[1:].replace('.', '')
        else:
            pennies = field.replace('.', '')
        return int(pennies)
    elif field:
        return field
    else:
        return None

def format_text_field(field):
    '''Cleans up txt fields (address, store name, city) to each word capitalized and striped of apostraphies'''
    if field:
        each_word_upper_str = ' '.join([word.capitalize() for word in field.strip().split(' ')])
        return each_word_upper_str
    else:
        return None

def parse_a_row(row):
    row[0] = format_int_field(row[0])
    row[1] = format_date_field(row[1])
    row[2] = format_int_field(row[2])
    row[3] = format_text_field(row[3])
    row[4] = format_text_field(row[4])
    row[5] = format_text_field(row[5])
    row[6] = format_int_field(row[6])
    
    lat, long = format_lat_long(row[7])
    row[7] = float(lat)
    row.insert(8, long)

    row[9] = format_int_field(row[9])
    row[11] = format_int_field(row[11])
    row[12] = format_text_field(row[11])
    row[13] = format_int_field(row[13])
    row[14] = format_text_field(row[14])
    row[15] = format_int_field(row[15])
    row[16] = format_text_field(row[16])
    row[17] = format_int_field(row[17])
    row[18] = format_int_field(row[18])
    row[19] = format_money_field(row[19])
    row[20] = format_money_field(row[20])
    row[21] = format_int_field(row[21])
    row[22] = format_money_field(row[22])
    row[23] = format_liter_to_ml(row)

    return row

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

if __name__ == '__main__':

    start = time.time()

    target_db = 'sales_new_seed.db'

    create_all_tables(target_db)

    abs_path_of_source_data = build_path(r'\input\iowa-liquor-sales\Iowa_Liquor_Sales.csv')
    raw_data_generator = read_csv(abs_path_of_source_data)

