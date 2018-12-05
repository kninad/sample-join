"""
Base class for all tables.
"""
import logging

log = logging.getLogger(__name__)

class Table:
    def __init__(self, name, columns=[], indexes=[]):
        self.data = {} # all data in this table.
        self.index = {} # all indexed attribute pointers
        self.columns = columns # names of all attributes.
        self.indexes = indexes # names of attributes to have a hash index on
        self.name = name
        self.create_table()

    def create_table(self):
        for col in self.columns:
            self.data[col] = []
        for index in self.indexes:
            if index in self.data:
                self.index[index] = {}
        
    def insert_tuple(self, tuple_dict):
        for k, v in tuple_dict:
            self.data[k].append(v)
            self.insert_into_index(k, v)

    def insert_list(self, data):
        if len(data) != len(self.columns):
            print(self.columns)
            print(data)
            raise ValueError('Data length does not match column length')
        for i in range(len(self.columns)):
            self.data[self.columns[i]].append(data[i])
            self.insert_into_index(self.columns[i], data[i])

    def insert_into_index(self, column, value):
        if column in self.index:
            if value not in self.index[column]:
                self.index[column][value] = []
            pointer = len(self.data[column]) - 1
            self.index[column][value].append(pointer)

    def get_name(self):
        return self.name

    def get_columns(self):
        return self.columns

    def has_index(self, column):
        return column in self.index

    def iterate_column(self, column):
        i = 0
        while i < len(self.data[column]):
            yield i, self.data[column][i]
            i += 1

    def get_row(self, index):
        return [self.data[c][index] for c in self.columns]
    

    def get_freq(self, column, value):
        return len(self.index[column][value])
    
    def get_max_freq(self, column):
        maxval = 0
        for key in self.index[column].keys():
            val = len(self.index[column][key])
            if val >= maxval:
                maxval = val
        return maxval


def make_table(name, column_list=[], indexes=[]):
    log.info("Creating table: %s"%name)
    log.info("%s contains columns: %s"%(name, column_list))
    return Table(name, columns=column_list, indexes=indexes)

        

if __name__=="__main__":
    
    logging.basicConfig(filename="log.txt", level=logging.INFO)
    x = make_table(name="Test", column_list=["a", "b","c"])
