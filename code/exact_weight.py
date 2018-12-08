"""
Implementation of Exact Weight algorithms.
"""
import numpy as np


def compute_w_t(table_list, join_columns, join_idx=None, table_idx=None):

    if table_idx is None:
        # start the computation.
        table_idx = len(table_list)-1
        current_table = table_list[table_idx]
        n_tuples = len(current_table.data["tuple_weight"])
        current_table.data["tuple_weight"] = [1]*n_tuples
        # setting w(t) for all tuples in the final table as 1.

        join_idx = len(join_columns) - 1

        compute_w_t(table_list, join_columns, join_idx, table_idx-1) # go to one table previous.

    elif table_idx>=0:
        current_table = table_list[table_idx]
        join_column = join_columns[join_idx]

        next_table = table_list[table_idx+1]

        for t_idx, t_val in enumerate(current_table.data[join_column]):

            matching_t_idx = next_table.index[join_column][t_val]
            # all tuples in the next table that have t_val in the join column.

            w_t = [next_table.data["tuple_weights"][idx] for idx in matching_t_idx]

            current_table.data["tuple_weights"][t_idx] = sum(w_t)
            compute_w_t(table_list, join_columns, join_idx-1, table_idx-1)
    else:
        return


def get_chaudhuri_sample_2way(table1, table2, join_column):
    """
    Basic 2way Chaudhuri sample join.
    :param table1:
    :param table2:
    :param join_column:
    :return:
    """

    candidates = table2.index[join_column].keys()
    frequencies = []
    for k in candidates:
        current_freq = len(table2.index[join_column][k])
        frequencies.append(current_freq)

    probabilities = np.array(frequencies, dtype=np.float32)/np.sum(frequencies)

    idx = np.random.choice(len(candidates), p=probabilities)

    desired_candidate = candidates[idx]

    possible_t1_tuples = table1.index[join_column][desired_candidate]
    possible_t2_tuples = table2.index[join_column][desired_candidate]

    # all tuples with join_col = desired candidate

    t1_sample_idx = np.random.choice(possible_t1_tuples)
    t2_sample_idx = np.random.choice(possible_t2_tuples)

    tuple1 = table1.get_row(t1_sample_idx)
    tuple2 = table2.get_row(t2_sample_idx)

    return (t1_sample_idx, t2_sample_idx), (tuple1, tuple2)

