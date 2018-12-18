
from __future__ import division
import numpy as np

from generalizing_olken import GeneralizedOlkens
from extended_olken import ExtendedOlkens
from exact_weight import ExactWeight

# def full_weight_table(join_index, wtcomp_obj):
#     table = wtcomp_obj.table_pairs[join_index][0]
#     colmn = wtcomp_obj.join_pairs[join_index][0]   # join column
#     total_len = len(table.data[colmn])
#     weight_sum = 0
#     for i in range(total_len):
#         weight_sum += wtcomp_obj.compute_tuple_weight(i, join_index)
#     return weight_sum


def get_tuple(t_curr, join_index, wtcomp_obj, weight_semi_join, base_flag):
    if base_flag:
        assert join_index == 0
        accept = False
        table = wtcomp_obj.table_pairs[0][0]
        colmn = wtcomp_obj.join_pairs[0][0]
        N = len(table.data[colmn])
        while not accept:
            rand_idx = np.random.randint(low=0, high=N)
            w_rand_idx = wtcomp_obj.compute_tuple_weight(rand_idx, join_index)
            accept_prob = w_rand_idx * 1.0 / weight_semi_join
            random_toss = np.random.random()
            if random_toss <= accept_prob:
                accept = True
                return rand_idx
    else:
        accept = False
        prev_table, table = wtcomp_obj.table_pairs[join_index]
        prev_colmn, colmn = wtcomp_obj.join_pairs[join_index]
        value = prev_table.data[colmn][t_curr]
        while not accept:
            N = len(table.index[colmn][value])   # its a list
            tmp = np.random.randint(low=0, high=N)
            rand_idx = table.index[colmn][value][tmp]
            if join_index == len(wtcomp_obj.table_pairs) - 1:   # LAST TABLE R_n
                w_rand_idx = 1.0
            else:
                w_rand_idx = wtcomp_obj.compute_tuple_weight(rand_idx, join_index + 1)  # should it be join_index + 1
                        
            accept_prob = w_rand_idx * 1.0 / weight_semi_join
            random_toss = np.random.random()
            if random_toss <= accept_prob:
                accept = True
                return rand_idx
    return


def get_single_sample(table_pairs, join_pairs, wtcomp_obj):    
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
        # accept = False
        w_prime = w_t_curr
        curr_join_index = i

        # compute the semi-join weight and set it to w_t_curr
        w_t_curr = wtcomp_obj.compute_relation_weight(t_curr, curr_join_index)
        accept_prob = w_t_curr * 1.0 / w_prime
        random_toss = np.random.random()
        if random_toss <= accept_prob:
            #accept
            t_curr = get_tuple(t_curr, curr_join_index, wtcomp_obj, w_t_curr, base_flag=False)
            join_sample.append(t_curr)
        else:
            # print('Rejected at stage: ', i)
            flag_no_reject = False
            break

    return flag_no_reject, join_sample


def sampler(num_samples, method, table_pairs, join_pairs):    
    
    # initialize the weight computation object
    if method == 'Generalized-Olken':
        wtcomp_obj = GeneralizedOlkens(table_pairs, join_pairs)
    elif method == 'Extended-Olken':
        wtcomp_obj = ExtendedOlkens(table_pairs, join_pairs)
    elif method == 'Exact-Weight':
        wtcomp_obj = ExactWeight(table_pairs, join_pairs)

    samples_list = []
    for t in range(num_samples):
        tmp_flag = False
        tmp_samp = None
        while not tmp_flag:
            tmp_flag, tmp_samp = get_single_sample(table_pairs, join_pairs, wtcomp_obj)
        
        samples_list.append(tmp_samp)
    
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

    


"""
# OLD CODE

def old_compute_w_t(table_list, join_columns):
    W_t = 1
    for i in range(len(table_list)):
        W_t *= table_list[i].get_max_freq(join_columns[i])
    return W_t


def old_sampler(table_list, join_columns, method=None):

    def get_random_tuple(t_old, join_col, table, weight_semi_join, base_flag):
        retval = None
        if base_flag:
            accept = False    
            while not accept:
                N =  len(table.data[join_col])
                rand_idx = np.random.randint(low=0, high=N)
                w_t_new = compute_tuple_weight(rand_idx, table, join_col, table_list, join_columns)
                accept_prob = w_t_new/weight_semi_join
                random_toss = np.random.random()
                if random_toss <= accept_prob:
                    accept = True
                    retval = rand_idx            
        else:
            accept = False    
            while not accept:
                N = len(table.index[join_col][t_old])   # its a list
                rand_idx = np.random.randint(low=0, high=N)
                w_t_new = compute_tuple_weight(rand_idx, table, join_col, table_list, join_columns)
                accept_prob = w_t_new/weight_semi_join
                random_toss = np.random.random()
                if random_toss <= accept_prob:
                    accept = True
                    retval = rand_idx

        return retval

    t_curr = None    # indices i.e iteration number    

    N = len(table_list)
    total_sample = []
    for i in range(N):
        if i == 0:
            accept = True
            weight_semi_join = compute_total(table_list[i])
            weight_t_curr = weight_semi_join            
            table1 = table_list[0]
            column1 = join_columns[0][0]    

            base_flag = True
            tmp = get_random_tuple(t_curr, column1, table1, weight_semi_join, base_flag)
            t_curr = tmp            
            total_sample.append(t_curr)
        else:
            accept = False           
            tab_t_curr = table_list[i-1]
            col_t_curr = join_columns[i-1][0]
            # W'(t) <- W(t)            
            weight_prime = weight_t_curr

            # W(t) <- W(semi-join)
            col_to_join_on = join_columns[i-1][1]
            tab_to_join_on = table_list[i]
            weight_semi_join = compute_relation_weight(t_curr, tab_to_join_on, col_to_join_on, table_list, join_columns)
            weight_t_curr = weight_semi_join

            random_toss = np.random.random()
            accept_prob = weight_semi_join/weight_prime            
            if random_toss <= accept_prob:
                accept = True
        
            if accept:
                base_flag = False
                tmp = get_random_tuple(t_curr, col_to_join_on, tab_to_join_on, weight_semi_join, base_flag)
                t_curr = tmp
                total_sample.append(t_curr)
            else:
                print('getting out of the loop')
                break


    return total_sample
"""
