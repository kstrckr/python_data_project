import csv
import os

def build_path(filename):
    script_dir = os.path.dirname(__file__)
    rel_path = filename
    abs_file_path = os.path.join(script_dir, rel_path)

    return abs_file_path

def read_csv(filename, year):
    print('Generator Initiated')
    with open(filename, 'r') as raw_data:
        data_reader = csv.reader(raw_data)

        for row in data_reader:
            # if row[1][6:] == year:
            yield row
            # else:
            #     continue
    print('Generator Complete')

def batch_stores_output(year):
    output = []
    raw_data_generator = read_csv(abs_path, year)

    for row in raw_data_generator:
        if not any(row[2] in sl for sl in output):
            print('added {} to store list'.format(row[3]))
            output.append(row[2:10])
    return output
    
abs_path = build_path(r'iowa-liquor-sales\Iowa_Liquor_Sales.csv')
annual_sale = batch_stores_output('2015')


print(annual_sale[-1])
print(len(annual_sale))


