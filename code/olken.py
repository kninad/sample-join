
from collections import defaultdict, Counter
import numpy as np

'''
TODO:
- statistics in table as an attribute
- using the already built-in index to compute the attributes
'''


def get_freq(table, col_idx):
    col_list = table.data[col_idx]
    count_dict = Counter(col_list)
    max_elem = Counter.most_common(1)
    

def find_matching_tuple(table, val, join_col):
    cols = table.get_columns()
    idx = cols.index(join_col) # list method to find the index
    

def get_uniform_sample(table1, table2, join_column):
    N1 = len(table1.data[join_column])
    
    rand_idx1 = np.random.randint(low=0, high=N1)    
    t1_val = table1.data[join_column][rand_idx1]
    joining_tups = table2.index[join_column][t1_val]  # using the table-index
    rand_idx2 = np.random.randint(low=0, high=len(rand_idx2))

    # Get the frquency of t1_val in table2 (in the join_column)
    freq_v = table2.get_freq(join_column, t1_val)  
    maxval = table2.get_max_freq(join_column)

    accept_prob = freq_v * 1.0 / maxval
    ret_flag = False
    while(not ret_flag):
        random_toss = np.random.random_sample()
        if random_toss <= accept_prob:
            ret_flag
            return (rand_idx1, rand_idx2)














def olken_sample_join(table1, table2, join_col):
    '''
    Func to emulate olken
    
    Args: table1, table2 -- TPCH.table objects
          join_col (str): the col over which to take the join.
    '''
    cols1 = table1.get_columns()
    cols2 = table2.get_columns()      
    idx1 = cols1.index(join_col)
    idx2 = cols2.index(join_col)
    
    N1 = len(table1.data[join_col])
    N2 = len(table2.data[join_col])
    
    

    



