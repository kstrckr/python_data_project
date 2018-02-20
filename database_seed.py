import csv
import os
import re

import db_setup as setup


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
        # row = next(data_reader)
        # yield row
        counter = 0
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
        null_string = 'NULL'
        lat = null_string
        long = null_string
    
    return (lat, long)



def batch_stores_output(data_input):
    '''the for loop in this function checks against some condition, in this case Store Number
    and adds the store details to a dictionary (key = Store Number)'''
    output_complete = {}
    output_incomplete = {}

    for row in data_input:

        if not row[2] in output_complete:

            lat, long = parse_lat_long(row[7].replace('\n', ' '))

            row[7] = lat
            row.insert(8, long)

            for i, data in enumerate(row):
                row[i] = row[i].replace('"', '')
                if not data:
                    row[i] = 'NULL'
                    if row[-1] != 'flagged':
                        row.append('flagged')

            if row[-1] == 'flagged':
                
                if row[2] in output_incomplete:
                    for i, data in enumerate(row[2:11]):
                        if output_incomplete[row[2]][i] != row[i]:
                            output_incomplete[row[2]][i] = row[i]

                output_incomplete[row[2]] = row[2:11]
                print('flagged row = ', output_incomplete[row[2]])

            else:
                output_complete[row[2]] = row[2:11]
                print('parsed row = ', output_complete[row[2]])

    print(len(output_complete), " ", len(output_incomplete))

    for key, value in output_incomplete.items():
        if not key in output_complete:
            output_complete[key] = value
        print(len(output_complete))
    
    return output_complete

def single_store_output(data_input, store_number):

    output = {}

    for row in (data_input):

        if row[2] == store_number:

            lat, long = parse_lat_long(row[7].replace('\n', ' '))

            row[7] = lat
            row.insert(8, long)

            for i, data in enumerate(row):
                row[i] = row[i].replace('"', '')
                if not data:
                    row[i] = 'NULL'

            output[row[2]] = row[2:11]
            print('parsed row = ', output[row[2]])
    
    return output
    

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
            'county_name': data[8],
        }

        values = '{}, "{}", "{}", "{}", {}, {}, {}, {}, "{}"'.format(
                                row['store_number'],
                                row['store_name'],
                                row['address'],
                                row['city'],
                                row['zip_code'],
                                row['store_lat'],
                                row['store_long'],
                                row['county_number'],
                                row['county_name'])

        properly_nulled_values = values.replace(r'"NULL"', 'NULL')

        command = '''INSERT INTO stores VALUES ({})'''.format(properly_nulled_values)

        print(command)

        database.db.execute(command)
        
        
        print('Inserted {} into database'.format(row['store_name']))

    database.db.commit()
    print('Inserts Committed')

def seed_unique_stores():

    database = setup.IowaLiquorDB('sales_db.db')

    stores_table = setup.IowaLiquorStoresTable()

    database.db.execute(stores_table.create_table())

    abs_path_of_source_data = build_path(r'iowa-liquor-sales\Iowa_Liquor_Sales.csv')
    raw_data_generator = read_csv(abs_path_of_source_data)
    all_unique_stores = batch_stores_output(raw_data_generator)

    insert_stores(all_unique_stores, database)

def seed_single_store():

    database = setup.IowaLiquorDB('single_store.db')
    stores_table = setup.IowaLiquorStoresTable()
    database.db.execute(stores_table.create_table())
    
    abs_path_of_source_data = build_path(r'iowa-liquor-sales\Iowa_Liquor_Sales.csv')
    raw_data_generator = read_csv(abs_path_of_source_data)

    all_single_store_number = single_store_output(raw_data_generator, '5336')

    insert_stores(all_single_store_number, database)

# seed_single_store()
seed_unique_stores()
