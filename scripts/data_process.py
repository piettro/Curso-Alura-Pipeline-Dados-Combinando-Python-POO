import json
import csv

class Data:
    def __init__(self, path, data_type):
        self.path = path
        self.data_type = data_type
        self.data = self.data_reader()
        self.columns_names = self.get_columns()
        self.len_rows = self.size()

    def data_reader(self):
        data = []

        if self.data_type == 'json':
            data = self.json_reader()
        elif self.data_type == 'csv':
            data = self.csv_reader()
        elif self.data_type == 'list':
            data = self.path
            self.path = 'List in memory'
        
        return data
    
    def csv_reader(self):
        data = []
        with open(self.path, 'r') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                data.append(row)
        return data

    def json_reader(self):
        data = []
        with open(self.path, 'r') as file:
            data = json.load(file)
        return data

    def get_columns(self):
        return list(self.data[-1].keys())

    def rename_columns(self, key_mapping):
        new_data = []

        for old_dict in self.data:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value

            new_data.append(dict_temp)
            
        self.data = new_data
        self.columns_names = self.get_columns()

    def size(self):
        return len(self.data)

    def join(data_a, data_b):
        joined_data = []
        joined_data.extend(data_a.data)
        joined_data.extend(data_b.data)

        return Data(joined_data, 'list')

    def transform_data_table(self):
        combined_data_table = [self.columns_names]

        for row in self.data:
            new_row = []
            for column in self.columns_names:
                new_row.append(row.get(column, 'Unavailable'))

            combined_data_table.append(new_row)
        
        return combined_data_table

    def saving_data(self, path):
        data_combined_table = self.transform_data_table()

        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(data_combined_table)
