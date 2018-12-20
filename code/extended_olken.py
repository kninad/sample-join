from copy import deepcopy

class ExtendedOlkens:

    def __init__(self, table_pairs, join_pairs):
        self.table_pairs = table_pairs
        self.join_pairs = join_pairs
        self.permutations = {}
        self.__build_table_permutations()

    def compute_tuple_weight(self, tuple_index, join_index):
        '''
        Compute the weight of the join on a tuple in a table with other
        tables

        tuple_index: int - index of the tuple to join with in self.table_pairs
        join_index: int - index of the tables to join with in self.join_pairs

        RETURNS int - estimated join cardinality
        '''
        try:
            return self.permutations[join_index + 1]:
        except: # final table requested
            return self.table_pairs[-1][1].get_count()

    def compute_relation_weight(self, tuple_index, join_index):
        '''
        Compute the weight of the semi-join between the tuple at tuple_index
        and the join table its associated with

        tuple_index: int - index of the tuple to join with in self.table_pairs
        join_index: int - index of the tables to join with in self.join_pairs

        RETURNS int - estimated join cardinality
        '''
        pass
        # get value of column in input tuple
        tuple_column = self.join_pairs[join_index][0]
        tuple_table = self.table_pairs[join_index][0]
        column_value = tuple_table.data[tuple_column][tuple_index]

        # find number of matching tuples in semi-join
        join_column = self.join_pairs[join_index][1]
        join_table = self.table_pairs[join_index][1]
        matching_tuples = join_table.index[join_column].get(column_value, [])

        # return tuple weight times cardinality of semi-join
        tuple_weight = self.compute_tuple_weight(tuple_index+1, join_index+1)
        return tuple_weight * len(matching_tuples)

    def compute_total_weight(self):
        '''
        Compute the weight of the full join

        RETURNS int - estimated join cardinality
        '''      
        num_tuples = self.table_pairs[0][0].get_count()
        weight_tuple = self.compute_tuple_weight(0, 0)
        return num_tuples * weight_tuple

    def __build_table_permutations(self):
        indices = list(range(len(self.table_pairs) + 1))
        for i in range(len(indices)):
            to_consider = list(range(i+1, len(self.table_pairs)))
            to_consider = [(j,j+1) for j in to_consider]
            self.__reursive_selection(to_consider, i, len(indices)-1)
        for key in self.permutations:
            l = []
            for value in self.permutations[key]:
                try:
                    value = set([int(x) for x in value.split('-')])
                    l.append(value)
                except:
                    pass # non integer value we want to get rid of anyway
            to_remove = set()
            for j in range(len(l)):
                for k in range(j+1, len(l)):
                    if j == k:
                        continue
                    if l[j].issubset(l[k]) and l[j] != l[k]:
                        to_remove.add(k)
                    if l[k].issubset(l[j]) and l[j] != l[k]:
                        to_remove.add(j)
            to_remove = sorted(list(to_remove), reverse=True)
            for m in to_remove:
                del l[m]
            self.permutations[key] = l
        self.__precompute_values()

    def __reursive_selection(self, choices, start_index, end_index, my_choices=set()):
        # base case, no more choices
        if len(choices) == 0:
            my_choices.add(start_index)
            my_choices.add(end_index)
            if start_index not in self.permutations:
                self.permutations[start_index] = set()
            s = '-'.join([str(n) for n in sorted(list(my_choices))])
            self.permutations[start_index].add(s)
            return
        # select choice 1
        my_choices_1 = deepcopy(my_choices)
        my_choices_1.add(choices[0][0])
        self.__reursive_selection(choices[1:], start_index, end_index, my_choices=my_choices_1)
        # select choice 2
        my_choices_2 = deepcopy(my_choices)
        my_choices_2.add(choices[0][1])
        self.__reursive_selection(choices[1:], start_index, end_index, my_choices=my_choices_2)

    def __precompute_values(self):
        for index in self.permutations:
            W_t = float('inf')
            for table_index_set in self.permutations[index]:
                table_index = sorted(list(table_index_set))
                W_t_current = 1
                for table_index in table_index[:-1]:
                    W_t_current *= self.table_pairs[table_index][0].get_count()
                W_t_current *= self.table_pairs[-1][1].get_count()
                W_t = min(W_t, W_t_current)
            self.permutations[index] = W_t
