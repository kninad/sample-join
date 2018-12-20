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

        table1 = table_pairs[0][0]
        print sum(self.weights[table1.name])
        exit(0)

    def create_dictionary(self):
        """
        Creates the place holder dictionary for the weights.
        """

        for table_pair in self.table_pairs:
            table1, table2 = table_pair
            if table1.name not in self.weights:
                N1 = table1.get_count()
                self.weights[table1.name] = [0]*N1
            if table2.name not in self.weights:
                N2 = table2.get_count()
                self.weights[table2.name] = [0]*N2

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

            # print "R_n = %s. Weights = 1 for all tuples " % last_table.name

            self.weights[last_table.name] = [1]*last_table.get_count()
            self.compute_weights(join_idx)  # go to one table previous.

        elif not start and join_idx >= 0:

            current_table, next_table = self.table_pairs[join_idx]
            col1, col2 = self.join_pairs[join_idx]
            assert current_table.has_index(col1), "Missing index."
            assert next_table.has_index(col2), "Missing index."

            for b_val in next_table.index[col2]:
                w = 0
                for b_idx in next_table.index[col2][b_val]:
                    w += self.weights[next_table.name][b_idx]

                for a_idx in current_table.index[col1][b_val]:
                    self.weights[current_table.name][a_idx] = w

            self.compute_weights(join_idx-1)

        else:
            return

    def compute_tuple_weight(self, tuple_index, join_index):
        table1, _ = self.table_pairs[join_index]
        return self.weights[table1.name][tuple_index]

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
        w_t = 0
        for idx in matching_tuples:
           w_t += self.weights[table2.name][idx]
        return w_t

    def compute_total_weight(self):
        """
        Base case for the sampler.
        :param table:
        :return:
        """
        table1, _ = self.table_pairs[0]
        col1, _ = self.join_pairs[0]
        total = sum(self.weights[table1.name])
        return total


if __name__ == "__main__":
    pass


###############################################################################
# // And it's too late to lose the weight you used to need to throw around.// #
###############################################################################
