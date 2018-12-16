"""
Implementation of Exact Weight algorithms.
"""


class ExactWeight:

    def __init__(self, table_pairs, join_pairs):
        self.weights = dict()
        assert len(table_pairs) == len(join_pairs), "Ninad goofed up."
        self.table_pairs = table_pairs
        self.join_pairs = join_pairs
        self.create_dictionary()
        self.compute_weights(start=True)

    def create_dictionary(self):
        """
        Creates the place holder dictionary for the weights.
        """

        for table_pair, col_pair in zip(self.table_pairs, self.join_pairs):
            table1, table2 = table_pair
            col1, col2 = col_pair

            if table1.name in self.weights:
                self.weights[table1.name].update({col1: {}})
            else:
                self.weights[table1.name] = {col1: {}}

            if table2.name in self.weights:
                self.weights[table2.name].update({col2: {}})
            else:
                self.weights[table2.name] = {col2: {}}

    def compute_weights(self, join_idx=None, start=False):
        """

        :param table_list: List of table references.
        :param join_columns: List of join column names.
        :param join_idx: index of join in the join_columns list. T_0 and T_1 are joined by the column at join_idx= 0
        :param table_idx: index of table to compute weights on.
        :param weights: dictionary to cache the weights from the table to the right in the join sequence.
        :param start: flag to be set at the start of the computation.
        :return:
        """
        if start:
            # start the computation.
            join_idx = len(self.table_pairs)-1
            _, last_table = self.table_pairs[join_idx]

            join_idx = len(self.join_pairs) - 1
            current_join = self.join_pairs[join_idx]
            col1, col2 = current_join

            assert last_table.has_index(col2), "Missing index."
            distinct_values = last_table.index[col2].keys()

            for val in distinct_values:
                self.weights[last_table.name][col2][val] = 1
                # setting w(t) for all tuples in the final table's join column as 1.

            self.compute_weights(join_idx)  # go to one table previous.

        elif self.weights is not None and join_idx >= 0:

            current_table, next_table = self.table_pairs[join_idx]
            col1, col2 = self.join_pairs[join_idx]

            assert current_table.has_index(col1), "Missing index."
            assert next_table.has_index(col2), "Missing index."

            for t_val in current_table.index[col1].keys():  # distinct values in the join column.

                w_t = self.weights[next_table.name][col2].get(t_val, 0)  # Thanks Corey.

                if w_t > 0:
                    n_occurences = len(next_table.index[col2][t_val])
                    w_t_new = w_t*n_occurences
                else:
                    w_t_new = 0

                self.weights[current_table.name][col1][t_val] = w_t_new
                self.compute_weights(join_idx-1)

        else:
            return

    def compute_tuple_weight(self, tuple_index, join_index):
        table1, _ = self.table_pairs[join_index]
        # print table1.index['REGIONKEY']
        col1, _ = self.join_pairs[join_index]
        t_val = table1.data[col1][tuple_index]
        # print table1.name, col1, t_val
        return self.weights[table1.name][col1][t_val]

    def compute_relation_weight(self, tuple_index, join_index):
        """
        Semi join.
        :param t_idx:
        :param table:
        :param join_col:
        :return:
        """
        table1, table2 = self.table_pairs[join_index]
        col1, col2 = self.join_pairs[join_index]

        t_val = table1.data[col1][tuple_index]
        matching_tuples = table2.index[col2].get(t_val, [])
        n_occurences = len(matching_tuples)

        w_t = self.weights[table2.name][col2].get(t_val, 0)

        return w_t * n_occurences

    def compute_total_weight(self):
        """
        Base case for the sampler.
        :param table:
        :return:
        """
        table1, _ = self.table_pairs[0]
        col1, _ = self.join_pairs[0]

        if len(self.weights[table1.name].keys()) > 1:
            print "Warning, there are more than two columns in this weights table."

        total = 0
        print table1.name, col1
        print self.weights[table1.name][col1]
        print self.weights
        for _, v in self.weights[table1.name][col1].items():
            total += v
        return total


if __name__=="__main__":
    pass


###############################################################################
# // And it's too late to lose the weight you used to need to throw around.// #
###############################################################################
