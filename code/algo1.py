import numpy as np

def compute_w_t(table_list, join_columns):
    W_t = 1
    for i in range(len(table_list)):
        W_t *= table_list[i].get_max_freq(join_columns[i])
    return W_t



def sampler(table_list, join_columns, method=None):

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
 