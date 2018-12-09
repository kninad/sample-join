def compute_w_t(table_list, join_columns):
    W_t = 1
    for i in range(len(table_list)):
        W_t *= table_list[i].get_max_freq(join_columns[i])
    return W_t
