from torch.utils.data import Dataset, DataLoader
import torch
from argparse import ArgumentParser
import numpy as np
import time
import os, logging
import TPCH
from algo1 import get_single_sample, verify_tuple

from generalizing_olken import GeneralizedOlkens
from extended_olken import ExtendedOlkens
from exact_weight import ExactWeight
from utils import *

log = logging.getLogger(__name__)


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
    log.info("Skipping PART and PARTSUPP table.")
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

        if method == 'Generalizing-Olken':
            self.wtcomp_obj = GeneralizedOlkens(table_pairs, join_pairs)
        elif method == 'Extended-Olken':
            self.wtcomp_obj = ExtendedOlkens(table_pairs, join_pairs)
        elif method == 'Exact-Weight':
            self.wtcomp_obj = ExactWeight(table_pairs, join_pairs)
        else:
            raise Exception("Invalid method argument")

        self.table_pairs = table_pairs
        self.join_pairs = join_pairs
        self.table_list = table_list

    def __len__(self):
        return self.n_samples

    def __getitem__(self, idx):
        """
        :param idx: index of sample clip in dataset
        :return: Gets the required sample, and ensures only 9 frames from the clip are returned.
        """
        start = time.time()
        tmp_flag = False
        tmp_samp = None
        while not tmp_flag:
            tmp_flag, tmp_samp = get_single_sample(self.table_pairs, self.join_pairs, self.wtcomp_obj)
        stop = time.time()
        elapsed_time = stop - start
        return elapsed_time
        # tuple_list = []
        # for idx, t_idx in enumerate(tmp_samp):
        #     current_table = self.table_list[idx]
        #     aTuple = current_table.get_row_dict(t_idx)
        #     tuple_list.append(aTuple)
        #     # print aTuple, current_table.name
        #
        # assert verify_tuple(tuple_list, self.join_pairs), "Verification failed."
        # return True


def data_generator(config, method, table_list, table_pairs, join_pairs):

    batch_size = config.getint("MISC", "BATCH_SIZE")
    n_workers = config.getint("MISC", "N_WORKERS")

    ps = ParallelSampler(config, method, table_list, table_pairs, join_pairs)

    sample_generator = DataLoader(ps, batch_size=batch_size, num_workers=n_workers,
                                  worker_init_fn=lambda _: np.random.seed(int(torch.initial_seed()%(2**32 -1))))

    return sample_generator


def get_samples(database, config):
    query_id, num_samp, method, n_trials = get_params(config)
    assert query_id in QUERIES, "Invalid query id."

    table_list = QUERIES[query_id]['TABLE_LIST']
    join_pairs = QUERIES[query_id]['JOIN_PAIRS']

    table_list = [database.tables[table_name] for table_name in table_list]

    table_pairs = zip(table_list[:-1], table_list[1:])

    samps = data_generator(config, method, table_list, table_pairs, join_pairs)

    total_time = 0

    for time_per_sample in samps:
        total_time += time_per_sample.sum()

    return total_time


if __name__ == "__main__":
    args = getargs()
    config = read_config(args.config)

    new_db = TPCH.get_schema(config)
    load_db(new_db, config)
    logging.basicConfig(filename=args.log, level=logging.INFO)
    query_id, num_samp, method, n_trials = get_params(config)
    log.info("QUERY %s" % query_id)
    log.info("Getting %s samples using %s method. " % (num_samp, method))
    log.info("Number of trials: %s" % (n_trials))

    time_per_trial = []
    for i in range(n_trials):
        elapsed_time = get_samples(new_db, config)
        time_per_trial.append(elapsed_time)
        log.info("Trial %s: %.3f seconds. "%(i, elapsed_time))

    total_time = np.sum(time_per_trial)
    avg_time = np.mean(time_per_trial)
    std_dev = np.std(time_per_trial)
    log.info("Total Time taken: %.3f seconds. " % (total_time))
    log.info("Average time to get %s samples: %.3f  +/- %.3f seconds." % (num_samp, avg_time, std_dev))