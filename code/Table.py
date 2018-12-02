"""
Base class for all tables.
"""
import logging

log = logging.getLogger(__name__)

class Table:
    def __init__(self, name, columns=[]):
        self.data = {} # all data in this table.
        self.columns = columns # names of all attributes.
        self.name = name
        self.create_table()
        

    def create_table(self):
        for col in self.columns:
            self.data[col] = []
        
    def insert_tuple(self, tuple_dict):
        for k, v in tuple_dict:
            self.data[k].append(v)

    def insert_list(self, data):
        if len(data) != len(self.columns):
            print(self.columns)
            print(data)
            raise ValueError('Data length does not match column length')
        for i in range(len(self.columns)):
            self.data[self.columns[i]].append(data[i])

def make_table(name, column_list=[]):
    log.info("Creating table: %s"%name)
    log.info("%s contains columns: %s"%(name, column_list))
    return Table(name, columns=column_list)

        

if __name__=="__main__":
    
    logging.basicConfig(filename="log.txt", level=logging.INFO)
    x = make_table(name="Test", column_list=["a", "b","c"])
