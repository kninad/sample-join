import ConfigParser

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

def get_params(config):
    num_samp = config.getint("EXPT", "N_SAMPLES")

    n_trials = config.getint("EXPT", "N_TRIALS")
    method = config.get("EXPT", "METHOD")
    query_id = config.get("EXPT", "QUERY")

    return query_id, num_samp, method, n_trials
