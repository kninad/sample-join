import numpy as np

'''
TODO:
- statistics in table as an attribute
- using the already built-in index to compute the attributes
- Set the MAX freq, NORMAL while a building the index 
'''

def get_olken_sample_2way(table1, table2, join_column):
    N1 = len(table1.data[join_column])
    retval = None
    flag = False
    while(not flag):
        rand_idx1 = np.random.randint(low=0, high=N1)    
        t1_val = table1.data[join_column][rand_idx1]
        joining_tups = table2.index[join_column][t1_val]  # using the table-index
        rand_idx2 = np.random.randint(low=0, high=len(joining_tups))

        # Get the frquency of t1_val in table2 (in the join_column)
        freq_v = table2.get_freq(join_column, t1_val)  
        maxval = table2.get_max_freq(join_column)

        accept_prob = freq_v * 1.0 / maxval
        random_toss = np.random.random_sample()
        if random_toss <= accept_prob:
            flag = True
            retval = (rand_idx1, rand_idx2)
    
    return retval

