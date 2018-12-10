class ExtendedOlkens:

    def __init__(self, tables, join_cols):
        pass

    def compute_tuple_weight(t_idx, table, join_col, tables, join_cols):
        '''
        Compute the weight of the join on a tuple in a table with other
        tables

        t_idx: int - index of the tuple to join with
        table: Table - table object that tuple with index t_idx lives in
        join_col: str - join column of tuple with t_idx to consider
        tables: List<Table> - list of table objects in full join
        join_cols: List<tuple> - list of tuples of pairs of join columns 
        '''
        W_t = 1
        passed_table = False
        for i in range(len(tables)):
            c_table = tables[i]
            if passed_table:
                c_column = join_cols[i-1][1] 
                W_t *= c_table.get_max_freq_for_column(c_column)
            else:
                if c_table.get_name() == table.get_name():
                    passed_table = True
        return W_t

    def compute_semi_join_weight(t_idx, table, join_col, tables, join_cols):
        params = [t_idx, table, join_col, tables, join_cols]
        tuple_weight = self.compute_tuple_weight(*params)
        passed_table = False
        for i in range(len(tables)):
            c_table = tables[i]
            if passed_table:
                c_column = join_cols[i-1][1] 
                value = table.data[c_column][t_idx]
                count = c_table.index[c_column].get(value, 0)
                return count * tuple_weight
            else:
                if c_table.get_name() == table.get_name():
                    passed_table = True
