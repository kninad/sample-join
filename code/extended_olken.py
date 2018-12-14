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
        pass

    def compute_relation_weight(self, tuple_index, join_index):
        '''
        Compute the weight of the semi-join between the tuple at tuple_index
        and the join table its associated with

        tuple_index: int - index of the tuple to join with in self.table_pairs
        join_index: int - index of the tables to join with in self.join_pairs

        RETURNS int - estimated join cardinality
        '''
        pass

    def compute_total_weight(self):
        '''
        Compute the weight of the full join

        RETURNS int - estimated join cardinality
        '''
        pass

    def __build_table_permutations(self):
        indices = list(range(len(self.table_pairs) + 1))
        for i in range(len(indices)):
            to_consider = indices[i:len(indices)-1]
            to_consider = [(j,j+1) for j in to_consider]
            self.__reursive_selection(to_consider, i)
        for key in self.permutations:
            l = []
            for value in self.permutations[key]:
                try:
                    value = [int(x) for x in value.split('-')]
                    l.append(value)
                except:
                    pass # non integer value we want to get rid of anyway
            self.permutations[key] = l

    def __reursive_selection(self, choices, start_index, my_choices=set()):
        # base case, no more choices
        if len(choices) == 0:
            if start_index not in self.permutations:
                self.permutations[start_index] = set()
            s = '-'.join([str(n) for n in sorted(list(my_choices))])
            self.permutations[start_index].add(s)
            return
        # select choice 1
        my_choices_1 = deepcopy(my_choices)
        my_choices_1.add(choices[0][0])
        self.__reursive_selection(choices[1:], start_index, my_choices=my_choices_1)
        # select choice 2
        my_choices_2 = deepcopy(my_choices)
        my_choices_2.add(choices[0][1])
        self.__reursive_selection(choices[1:], start_index, my_choices=my_choices_2)

tables = [(i, i+1) for i in range(10)]
cols = deepcopy(tables)
X = ExtendedOlkens(tables, cols)
print(X.permutations)
