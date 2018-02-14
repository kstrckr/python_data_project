import csv
import os


def build_path(filename):
    '''lets you use a relative path to the data, rather than requiring absolute'''
    script_dir = os.path.dirname(__file__)
    rel_path = filename
    abs_file_path = os.path.join(script_dir, rel_path)

    return abs_file_path

def read_csv(filename, year):
    '''a generator function that reads through the CSV file,
    yielding a single row as needed'''
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
    '''the for loop in this function checks against some condition, in this case Store Number
    and adds the store details to a dictionary (key = Store Number)'''
    output = {}
    raw_data_generator = read_csv(abs_path, year)

    for row in raw_data_generator:
        if not row[2] in output:
            output[row[2]] = row[2:10]
            print('added {} to store list'.format(row[3]))
    return output
    
abs_path = build_path(r'iowa-liquor-sales\Iowa_Liquor_Sales.csv')
all_unique_stores = batch_stores_output('2015')

# print(annual_sale[-1])
print(len(all_unique_stores))


