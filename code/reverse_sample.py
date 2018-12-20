from torch.utils.data import Dataset, DataLoader
import torch
import ConfigParser
from argparse import ArgumentParser
import numpy as np
import TPCH
import os

from algo1 import verify_tuple

from generalizing_olken import GeneralizedOlkens
from extended_olken import ExtendedOlkens
from exact_weight import ExactWeight

QUERIES = {'Q3': {'TABLE_LIST': ['customer', 'orders', 'lineitem'],
                  'JOIN_PAIRS': [('CUSTKEY', 'CUSTKEY'), ('ORDERKEY', 'ORDERKEY')]},
           'QX': {'TABLE_LIST': ['nation', 'supplier', 'customer', 'orders', 'lineitem'],
                  'JOIN_PAIRS': [('NATIONKEY','NATIONKEY'), ('NATIONKEY', 'NATIONKEY'), ('CUSTKEY', 'CUSTKEY'),
                                 ('ORDERKEY','ORDERKEY')]},
           'Test':{'TABLE_LIST': ['region', 'nation', 'supplier'],
                  'JOIN_PAIRS': [('REGIONKEY', 'REGIONKEY'), ('NATIONKEY', 'NATIONKEY')]}}


def read_config(configpath='config.ini'):
    config = ConfigParser.RawConfigParser()
    config.read(configpath)
    return config


def getargs():
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", default="config.ini", help="Path to config.ini file.")
    parser.add_argument("--log", default="log.txt", help="Path to log file.")
    parser.add_argument("--expt", default="Test", help="Expt name.")
    args = parser.parse_args()
    return args


def load_db(db, cfg):
    """
    Loads the database.
    :param config:
    :return:
    """
    datapath = cfg.get("DATA", "PATH")
    fnames = [x for x in os.listdir(datapath) if x.endswith('.tbl')]
    fnames = [x for x in fnames if x.split('.')[0] not in ['part', 'partsupp']]
    print "Skipping PART and PARTSUPP table."
    for fname in fnames:
        table = fname.split('.tbl')[0]
        f = open('%s/%s' % (datapath, fname), 'r')
        for line in f:
            data = line.split('|')[:-1]
            db.tables[table].insert_list(data)


class ParallelSampler(Dataset):

    def __init__(self, cfg, method, table_list, table_pairs, join_pairs):
        """
        :param cfg: Config file.
        :param split: TRAIN/VAL/TEST
        """
        self.cfg = cfg
        self.n_samples = cfg.getint("EXPT", "N_SAMPLES")

        self.table_pairs = table_pairs
        self.join_pairs = join_pairs
        self.reversed_table_pairs = list(reversed(table_pairs))
        self.reversed_join_pairs = list(reversed(join_pairs))
        self.table_list = table_list

    def __len__(self):
        return self.n_samples

    def __getitem__(self, idx):
        """
        :param idx: index of sample clip in dataset
        :return: Gets the required sample, and ensures only 9 frames from the clip are returned.
        """
        tmp_flag = False
        tmp_samp = None
        while not tmp_flag:
            tmp_flag, tmp_samp = self.get_one_sample()
        tuple_list = []

        for idx, t_idx in enumerate(tmp_samp):
            current_table = self.table_list[idx]
            aTuple = current_table.get_row_dict(t_idx)
            tuple_list.append(aTuple)

        assert verify_tuple(tuple_list, self.join_pairs), "Verification failed."

        return True

    def get_one_sample(self):
        sample = []

        col1, col2 = self.reversed_join_pairs[0]
        table1, table2 = self.reversed_table_pairs[0]

        sampled_idx = np.random.randint(0, table2.get_count())
        sample.append(sampled_idx)

        desired_value = table2.data[col2][sampled_idx]
        if desired_value not in table1.index[col1]:
            return False, None

        subset = table1.index[col1][desired_value]

        for idx in range(1, len(self.reversed_join_pairs)):
            sampled_idx = np.random.choice(subset)
            sample.append(sampled_idx)

            col1, col2 = self.reversed_join_pairs[idx]
            table1, table2 = self.reversed_table_pairs[idx]
            desired_value = table2.data[col2][sampled_idx]
            if desired_value in table1.index[col1]:
                subset = table1.index[col1][desired_value]
            else:
                foundSample = False
                return foundSample, None

        final_idx = np.random.choice(subset)
        sample.append(final_idx)
        foundSample = True
        sample = list(reversed(sample))
        assert len(sample)==len(self.table_list), "Incorrect number of tuples joined."
        return foundSample, sample


def data_generator(config, method, table_list, table_pairs, join_pairs):

    batch_size = config.getint("MISC", "BATCH_SIZE")
    n_workers = config.getint("MISC", "N_WORKERS")

    ps = ParallelSampler(config, method, table_list, table_pairs, join_pairs)

    sample_generator = DataLoader(ps, batch_size=batch_size, num_workers=n_workers,
                                  worker_init_fn=lambda _: np.random.seed(int(torch.initial_seed()%(2**32 -1))))

    return sample_generator


def get_params(config):
    num_samp = config.getint("EXPT", "N_SAMPLES")

    n_trials = config.getint("EXPT", "N_TRIALS")
    method = config.get("EXPT", "METHOD")
    query_id = config.get("EXPT", "QUERY")

    return query_id, num_samp, method, n_trials


def get_samples(database, config):
    query_id, num_samp, method, n_trials = get_params(config)
    assert query_id in QUERIES, "Invalid query id."

    table_list = QUERIES[query_id]['TABLE_LIST']
    join_pairs = QUERIES[query_id]['JOIN_PAIRS']

    table_list = [database.tables[table_name] for table_name in table_list]

    table_pairs = zip(table_list[:-1], table_list[1:])

    samps = data_generator(config, method, table_list, table_pairs, join_pairs)

    for aSample in samps:
        pass

    print "Trial complete."


if __name__ == "__main__":
    args = getargs()
    config = read_config(args.config)

    new_db = TPCH.get_schema(config)
    load_db(new_db, config)

    query_id, num_samp, method, n_trials = get_params(config)
    print "QUERY %s" % query_id
    print "Getting %s samples using %s method. " % (num_samp, method)
    print "Number of trials: %s" % (n_trials)

    import timeit

    elapsed_time = timeit.timeit("get_samples(new_db, config)",
                                 number=n_trials,
                                 setup="from __main__ import get_samples, new_db, config")
    average_time = elapsed_time / n_trials

    print "Total Time taken: %.3f seconds. " % (elapsed_time)
    print "Average time to get %s samples: %.3f seconds." % (num_samp, average_time)