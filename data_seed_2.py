
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

#-- CSV READERS
def read_csv_generator(filename):
    '''a generator function that reads through the CSV file,
    yielding a single row as needed'''
    print('Generator Initiated')
    with open(filename, 'r') as raw_data:
        data_reader = csv.reader(raw_data)

        next(data_reader)

        for row in data_reader:
            # if row[1][6:] == 2016:
            yield row
            # else:
            #     continue
    print('Generator Complete')

def read_csv_batch(filename, qty):

    with open(filename, 'r') as raw_data:
        data_reader = csv.reader(raw_data)

        next(data_reader)
        
        batch_of_lines = [next(data_reader) for row in range(qty)]
    
    return batch_of_lines

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

    #----   store fields
    if not row[2] in stores:
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
        stores[row[2]] = {
            'store_number': row[2],
            'store_name': row[3],
            'address': row[4],
            'city': row[5],
            'zip_code': row[6],
            'lat':row[7],
            'long': row[8],
            'county_number': row[9]
        }
    #----   cateogry fields
    if not row[11] in categories:
        row[11] = format_int_field(row[11])     # category number
        row[12] = format_text_field(row[12])    # category name

        categories[row[11]] = {
            'category_number': row[11],
            'category_name': row[12],
        }
    #----   vendor fields
    if not row[13] in vendors:
        row[13] = format_int_field(row[13])     # vendor number
        row[14] = format_text_field(row[14])    # vendor name

        vendors[row[13]] = {
            'vendor_number': row[13],
            'vendor_name': row[14]
        }
    #----   item fields
    if not row[15] in items:
        row[15] = format_int_field(row[15])     # item number
        row[16] = format_text_field(row[16])    # item description
        row[17] = format_int_field(row[17])     # pack qty
        row[18] = format_int_field(row[18])     # bottle volume ml
        row[19] = format_money_field(row[19])   # state wholesale cost
        row[20] = format_money_field(row[20])   # state retail cost

        items[row[15]] = {
            'item_number': row[15],
            'item_description': row[16],
            'pack_qty': row[17],
            'bottle_vol_ml': row[18],
            'state_wholesale_cost': row[19],
            'state_retail_cost': row[20]
        }

    #----   sale fields (1)
    if not row[0] in sales:
        row[0] = format_text_field(row[0])      # invoice num
        row[1] = format_date_field(row[1])      # date
        #----   sale fields (2)
        row[21] = format_int_field(row[21])     # quantity sold
        row[22] = format_money_field(row[22])   # sale ammount
        row[23] = format_liter_to_ml(row[23])   # sale volume in ml
        sales[row[0]] = {
            'invoice_id': row[0],
            'date': row[1],
            'qty_sold': row[21],
            'sale_ammount': row[22],
            'sale_vol_ml': row[23]
        }

def parse_x_rows(raw_row_generator, x):
    master_dict = {}
    counter = 0

    for row in raw_row_generator:
        counter += 1
        single_row = parse_a_row(row)
        master_dict[single_row[0]] = single_row

        if counter == x:
            return master_dict

def create_all_tables(db):
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

def read_then_parse(batch_of_rows):

    master_dict = {}

    for row in batch_of_rows:
        master_dict[row[0]] = parse_a_row(row)
    
    print(len(master_dict))

def build_virtual_db(raw_row_generator, csv_file_size):
    categories = {}
    items = {}
    sales = {}
    stores = {}
    vendors = {}
    # bytes_read = 0

    # count = 0
    for row in raw_row_generator:
        # count += 1
        # clear()
        # bytes_read += len(','.join(row).encode('utf-8'))
        parse_a_selective_row(row, categories, items, sales, stores, vendors)
        # print(round((bytes_read/csv_file_size)*100, 4))
        # if count == x:
        #     break
    return (categories, items, sales, stores, vendors)

#-- DATA INSERTS

def insert_sales(db, dict_of_sales):

if __name__ == '__main__':

    start = time.time()

    target_db = 'sales_new_seed.db'

    create_all_tables(target_db)

    abs_path_of_source_data = build_path(r'input\iowa-liquor-sales\Iowa_Liquor_Sales.csv')
    raw_data_generator = read_csv_generator(abs_path_of_source_data)
    csv_byte_size = os.path.getsize(abs_path_of_source_data)


    # counter = 3000000
    # many_rows = parse_x_rows(raw_data_generator, counter)

    # fail_row = [['INV-00090800001', '09/01/2016', '4725', "Casey's General Store #1548 / Ankeny", '', '', '', '', '', '', '1012100', 'Canadian Whiskies', '260', 'DIAGEO AMERICAS', '11296', 'Crown Royal', '12', '750', '$15.07', '$22.61', '10', '$22.61', '7.50', '1.98']]
    categories, items, sales, stores, vendors = build_virtual_db(raw_data_generator, csv_byte_size)
    # counter, full_year = parse_a_year(raw_data_generator, '2016')


    # read_then_parse(read_csv_batch(abs_path_of_source_data, counter))

    stop = time.time()
    print('{} rows took {} seconds'.format(len(sales), stop - start))
    print(len(categories), len(items), len(sales), len(stores), len(vendors))
