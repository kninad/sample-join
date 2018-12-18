
from __future__ import division
import numpy as np

from generalizing_olken import GeneralizedOlkens
from extended_olken import ExtendedOlkens
from exact_weight2 import ExactWeight


'''
TODO:
    1. CHECK THE DIVISION CODE SINCE ITS FUCKING PYTHON 2!
    2. Print out the weights for semi-joins and tuples and verify if the 
       algorithm is retrieving them correctly
'''


def get_tuple(t_curr, join_index, wtcomp_obj, weight_semi_join, base_flag):
    ''' Function to get a random tuple from the semi-join
    Args:
        t_curr: the current tuple's index (iteration number) in the table
        join_index: Iteration number in the pairs list. Useful for referring to tables under consideration
        wtcomp_obj: An object from weight computation class. Used to retrieve the weights.
        weight_semi_join: Semi-join weight used in line-7 of Algo-1
        base_flag: Whether its the base case or not
    
    Returns:
        rand_idx: A random tuple index (iteration number) from the semi-join (Check this)
    '''
    if base_flag:
        assert join_index == 0, 'For base case, join_index should be zero '
        accept = False
        table = wtcomp_obj.table_pairs[0][0]
        colmn = wtcomp_obj.join_pairs[0][0]
        # N = len(table.data[colmn])
        N = table.get_count()   # returns the length of the table
        while not accept:
            rand_idx = np.random.randint(low=0, high=N)
            w_rand_idx = wtcomp_obj.compute_tuple_weight(rand_idx, join_index)
            accept_prob = w_rand_idx * 1.0 / weight_semi_join
            random_toss = np.random.random()
            if random_toss <= accept_prob:
                accept = True
                break
        print "Sampled tuple from R_1."
        return rand_idx
    else:
        accept = False
        prev_table, table = wtcomp_obj.table_pairs[join_index]
        prev_colmn, colmn = wtcomp_obj.join_pairs[join_index]
        value = prev_table.data[prev_colmn][t_curr]
        
        # should not even come here since then weight_semi_join should be zero
        # since the current tuple matches with nothing in the next table
        # But adding the code just for a sanity check
        if value not in table.index[prev_colmn]:
            print('No value found in second table\'s index')
            return None
        
        while not accept:

            N = len(table.index[colmn][value])   # its a list
            tmp = np.random.randint(low=0, high=N)  # choose a random itration number in the index list
            rand_idx = table.index[colmn][value][tmp]   # retrieve the random value from the index using tmp

            # ALT: Just use another numpy function to directly sample from a
            # list with weighted probabilities.

            if join_index == len(wtcomp_obj.table_pairs) - 1:   # LAST TABLE R_n
                w_rand_idx = 1.0
            else:
                # VERIFY THIS STATMENT AGAIN
                # IS PROB GOING TO ZERO?
                w_rand_idx = wtcomp_obj.compute_tuple_weight(rand_idx, join_index + 1)  # should it be join_index + 1
                        
            accept_prob = w_rand_idx * 1.0 / weight_semi_join
            random_toss = np.random.random()
            if random_toss <= accept_prob:
                accept = True
                print('Successfully got a tuple! Join-index: %s'%join_index)
                return rand_idx
    return


def get_single_sample(table_pairs, join_pairs, wtcomp_obj):    
    '''Function to get a single sample from the full-join. So it will be a list
    of tuple indices from each of the table in the join query.
    
    Args:
        table_pairs: List of the table pairs in the join
        join_paris: List of which columns to perform the join on
        wtcomp_obj: Object from a weight computation class

    Returns:
        flag_no_reject: Boolean flag to indicate whether we successfully got 
                        a full sample or got rejected midway
        join_sample: The sample (list) from the join. May or may not be valid
                     depending on flag_no_reject but returning it anyways.
    '''
    N = len(table_pairs)    # N := n-1
    join_sample = []    
    flag_no_reject = True

    # BASE CASE
    t_curr = None
    w_prime = None
    # w_prime = wtcomp_obj.compute_total_weight()
    # w_t_curr = full_weight_table(0, wtcomp_obj)   # Since the base case

    w_t_curr = wtcomp_obj.compute_total_weight()
    t_curr = get_tuple(t_curr, 0, wtcomp_obj, w_t_curr, base_flag=True)
    join_sample.append(t_curr)
    
    for i in range(N):        
        curr_join_index = i
        # w_prime = w_t_curr        # WRONG INIT # CLARIFIED ON PIAZZA

        # w_prime is set to the weight of the current tuple        
        w_prime = wtcomp_obj.compute_tuple_weight(t_curr, curr_join_index)        

        # compute the semi-join weight and set it to w_t_curr
        w_semijoin = wtcomp_obj.compute_relation_weight(t_curr, curr_join_index)
        accept_prob = w_semijoin * 1.0 / w_prime
        random_toss = np.random.random()
        if random_toss <= accept_prob:            
            t_curr = get_tuple(t_curr, curr_join_index, wtcomp_obj, w_semijoin, base_flag=False)
            
            # Check if we get a valid tuple from the semi-join
            # Can also check whether w_semijoin is very near to zero 
            if t_curr:
                join_sample.append(t_curr)
            else:
                print('Rejected at stage: ', i)
                print(w_semijoin, w_prime, accept_prob)
                flag_no_reject = False
                break
        else:
            print('Rejected at stage: ', i)
            print(w_semijoin, w_prime, accept_prob)
            flag_no_reject = False
            break

    return flag_no_reject, join_sample


def sampler(num_samples, method, table_pairs, join_pairs):    
    '''Function to get desired number of samples from the full-join. 

    Args:
        num_samples: Number of required samples
        method: The method for the (approximate) weight computation
        table_pairs: List of the table pairs in the join
        join_paris: List of which columns to perform the join on        

    Returns:
        sample_list: A list of lists. Each element is a single (and valid) sample
                     from the full join result
    '''
    
    # initialize the weight computation object
    if method == 'Generalized-Olken':
        wtcomp_obj = GeneralizedOlkens(table_pairs, join_pairs)
    elif method == 'Extended-Olken':
        wtcomp_obj = ExtendedOlkens(table_pairs, join_pairs)
    elif method == 'Exact-Weight':
        wtcomp_obj = ExactWeight(table_pairs, join_pairs)

    samples_list = []
    print "Launching sampler."
    for t in range(num_samples):
        tmp_flag = False
        tmp_samp = None
        while not tmp_flag:
            tmp_flag, tmp_samp = get_single_sample(table_pairs, join_pairs, wtcomp_obj)
        
        samples_list.append(tmp_samp)
        print "Got %s samples."%len(samples_list)
    
    return samples_list


def compose_tuple(sample, table):
    aTuple = dict()
    for col, values in table.data.items():
        aTuple.update({col: values[sample]})
    return aTuple


def verify_tuple(tupleList, join_columns):

    tuplePairs = zip(tupleList[:-1], tupleList[1:])
    for idx, eachPair in enumerate(tuplePairs):
        col1, col2 = join_columns[idx]
        t1, t2 = eachPair
        if not t1[col1]==t2[col2]:
            return False

    return True

