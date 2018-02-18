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

        for row in data_reader:
            # if row[1][6:] == year:
            yield row
            # else:
            #     continue
    print('Generator Complete')

def parse_lat_long(data):
    '''this function takes a row of data, and an index to parse. It regex matches lattitude and longitude values and returns a tuple for lat, long'''
    pattern = re.compile(r'^.*\((?P<lat>-?\d+\.\d+), (?P<long>-?\d+\.\d+).*$')
    try:
        match = pattern.match(data)
        lat, long = match.groups()
    except AttributeError:
        lat = 'NULL'
        long = 'NULL'
    

    return (lat, long)



def batch_stores_output():
    '''the for loop in this function checks against some condition, in this case Store Number
    and adds the store details to a dictionary (key = Store Number)'''
    output = {}
    raw_data_generator = read_csv(abs_path)

    for row in raw_data_generator:

        if not row[2] in output:



            lat, long = parse_lat_long(row[7].replace('\n', ' '))

            row[7] = lat
            row.insert(8, long)


            for i, data in enumerate(row):
                row[i] = row[i].replace('"', '')
                if len(data) == 0:
                    row[i] = 'NULL'

            output[row[2]] = row[2:11]
            print(output[row[2]])

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

        values = values.replace(r'"NULL"', 'NULL')

        command = '''INSERT INTO stores VALUES ({})'''.format(values)

        print(command)

        database.db.execute(command)
        
        
        print('Inserted {} into database'.format(row['store_name']))

    database.db.commit()
    print('Inserts Committed')



database = setup.IowaLiquorDB('sales_db.db')

stores_table = setup.IowaLiquorStoresTable()

database.db.execute(stores_table.create_table())


abs_path = build_path(r'iowa-liquor-sales\Iowa_Liquor_Sales.csv')
all_unique_stores = batch_stores_output()

insert_stores(all_unique_stores, database)
