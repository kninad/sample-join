class GeneralizedOlkens:

    def __init__(self, table_pairs, join_pairs):
        self.table_pairs = table_pairs
        self.join_pairs = join_pairs

    def compute_tuple_weight(self, tuple_index, join_index):
        '''
        Compute the weight of the join on a tuple in a table with other
        tables

        tuple_index: int - index of the tuple to join with in self.table_pairs
        join_index: int - index of the tables to join with in self.join_pairs

        RETURNS int - estimated join cardinality
        '''
        W_t = 1
        for i in range(join_index, len(self.table_pairs)):
            table, column = self.table_pairs[i][1], self.join_pairs[i][1]
            W_t *= table.get_max_freq_for_column(column)
        return W_t

    def compute_relation_weight(self, tuple_index, join_index):
        '''
        Compute the weight of the semi-join between the tuple at tuple_index
        and the join table its associated with

        tuple_index: int - index of the tuple to join with in self.table_pairs
        join_index: int - index of the tables to join with in self.join_pairs

        RETURNS int - estimated join cardinality
        '''
        # get value of column in input tuple
        tuple_column = self.join_pairs[join_index][0]
        tuple_table = self.table_pairs[join_index][0]
        column_value = tuple_table.data[tuple_column][tuple_index]

        # find number of matching tuples in semi-join
        join_column = self.join_pairs[join_index][1]
        join_table = self.table_pairs[join_index][1]
        matching_tuples = join_table.index[join_column].get(column_value, [])

        # return tuple weight times cardinality of semi-join
        tuple_weight = self.compute_tuple_weight(tuple_index, join_index)
        return tuple_weight * len(matching_tuples)

    def compute_total_weight(self):
        '''
        Compute the weight of the full join

        RETURNS int - estimated join cardinality
        '''
        join_column = self.join_pairs[0][0]        
        num_tuples = len(self.table_pairs[0][0].data[join_column])
        weight_tuple = self.compute_tuple_weight(0, 0)
        return num_tuples * weight_tuple
