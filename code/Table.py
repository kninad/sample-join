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
        self.max_freq = {} # value and freq for max value occurence per column
        self.name = name
        self.create_table()

    def create_table(self):
        for col in self.columns:
            self.data[col] = []
        for index in self.indexes:
            if index in self.data:
                self.index[index] = {}

    def insert_list(self, data):
        if len(data) != len(self.columns):
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
            # count = len(self.index[column][value])
            # if count > self.max_freq[column]:
            #     self.max_freq[column] = count

    # def get_max_freq_for_column(self, column):
    #     if column not in self.max_freq:
    #         raise NameError('Column not in table index: <%s>' % column)
    #     return self.max_freq[column]

    def get_name(self):
        return self.name

    def get_columns(self, tbl_name=False):
        if not tbl_name or self.name is None:
            return self.columns
        else:
            return ['%s.%s' % (self.get_name(), c) for c in self.columns]

    def has_index(self, column):
        return column in self.index

    def iterate_column(self, column):
        for idx, data in enumerate(self.data[column]):
            yield idx, data

    def iterate_index(self, column, value):
        if column in self.index and value in self.index[column]:
            for idx in self.index[column][value]:
                yield idx

    def get_count(self):
        return len(self.data[self.columns[0]])

    def get_row(self, idx): # TODO: non-immutable object
        return [self.data[c][idx] for c in self.columns]


def make_table(name, column_list=[], indexes=[]):
    log.info("Creating table: %s"%name)
    log.info("%s contains columns: %s"%(name, column_list))
    return Table(name, columns=column_list, indexes=indexes)


if __name__=="__main__":
    
    logging.basicConfig(filename="log.txt", level=logging.INFO)
    x = make_table(name="Test", column_list=["a", "b","c"])
