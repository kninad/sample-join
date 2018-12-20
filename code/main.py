from algo1 import *
import ConfigParser
import os, logging
import argparse
import TPCH
import numpy as np
np.random.seed(42)
import timeit

log = logging.getLogger(__name__)


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
    parser = argparse.ArgumentParser()
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

    log.info("Accessing tables stored in: %s" % cfg.get("DATA", "PATH"))

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


def get_samples(database, query_id, n_samples, method):
    assert query_id in QUERIES, "Invalid query id."
    # print "Sampling for: %s" % query_id
    table_list = QUERIES[query_id]['TABLE_LIST']
    join_pairs = QUERIES[query_id]['JOIN_PAIRS']

    table_list = [database.tables[table_name] for table_name in table_list]

    table_pairs = zip(table_list[:-1], table_list[1:])

    for table_pair, col_pair in zip(table_pairs, join_pairs):
        col1, col2 = col_pair
        table1, table2 = table_pair
        # print "Joining %s.%s and %s.%s"%(table1.name, col1, table2.name, col2)

    samps = sampler(n_samples, method, table_pairs, join_pairs)

    for aSample in samps:
        tuple_list = []
        for idx, t_idx in enumerate(aSample):
            current_table = table_list[idx]
            aTuple = current_table.get_row_dict(t_idx)
            tuple_list.append(aTuple)
            # print aTuple, current_table.name

        assert verify_tuple(tuple_list, join_pairs), "Verification failed."


if __name__ == "__main__":

    args = getargs()
    logging.basicConfig(filename=args.log, level=logging.INFO)
    config = read_config(args.config)

    new_db = TPCH.get_schema(config)
    load_db(new_db, config)
    num_samp = config.getint("EXPT", "N_SAMPLES")

    n_trials = config.getint("EXPT", "N_TRIALS")
    method = config.get("EXPT", "METHOD")
    query_id = config.get("EXPT", "QUERY")

    log.info("QUERY %s"%query_id)
    log.info("Getting %s samples using %s method. "%(num_samp, method))
    log.info("Number of trials: %s"%(n_trials))

    elapsed_time = timeit.timeit("get_samples(new_db, query_id, n_samples=num_samp, method=method)",
                                 number=n_trials,
                                 setup="from __main__ import get_samples, new_db, query_id, num_samp, method")

    average_time = elapsed_time/n_trials

    log.info("Total Time taken: %.3f seconds. "%(elapsed_time))
    log.info("Average time to get %s samples: %.3f seconds."%(num_samp, average_time))
