import ConfigParser
import os, logging
import argparse
from Table import make_table
from join import *


log = logging.getLogger(__name__)


def read_config(configpath='config.ini'):
    config = ConfigParser.RawConfigParser()
    config.read(configpath)
    return config


def getargs():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", default="config.ini", help="Path to config.ini file.")
    parser.add_argument("--log", default="log.txt", help="Path to log file.")
    args = parser.parse_args()
    return args


class TPCH:

    def __init__(self):
        log.info("Initializing TPCH schema.")
        self.create_db()
    
    def create_db(self):
        self.tables = {}

        self.tables['customer'] = make_table("CUSTOMER", column_list=["CUSTKEY", "NAME", "ADDRESS", "NATIONKEY",
                                                                      "PHONE", "ACCTBAL", "MKTSEGMENT", "COMMENT"],
                                             indexes=['CUSTKEY'])

        self.tables['lineitem'] = make_table("LINEITEM", column_list=["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER",
                                                                      "QUANTITY", "EXTENDEDPRICE", "DISCOUNT", "TAX",
                                                                      "RETURNFLAG", "LINESTATUS", "SHIPDATE",
                                                                      "COMMITDATE", "RECEIPTDATE", "SHIPINSTRUCT",
                                                                      "SHIPMODE", "COMMENT"],
                                             indexes=['ORDERKEY', 'PARTKEY'])

        self.tables['nation'] = make_table("NATION", column_list=["NATIONKEY", "NAME", "REGIONKEY", "COMMENT"])

        self.tables['orders'] = make_table("ORDERS", column_list=["ORDERKEY", "CUSTKEY","ORDERSTATUS", "TOTALPRICE",
                                                                  "ORDERDATE", "ORDERPRIORITY", "CLERK", "SHIPPRIORITY",
                                                                  "COMMENT"], indexes=['CUSTKEY', 'ORDERKEY'])

        self.tables['part'] = make_table("PART", column_list=["PARTKEY", "NAME", "MFGR","BRAND", "TYPE", "SIZE",
                                                              "CONTAINER", "RETAILPRICE", "COMMENT"],
                                         indexes=['PARTKEY'])

        self.tables['partsupp'] = make_table("PARTSUPPLY", column_list= ["PARTKEY","SUPPKEY","AVAILQTY", "SUPPLYCOST",
                                                                         "COMMENT"])

        self.tables['region'] = make_table("REGION", column_list=["REGIONKEY",  "NAME","COMMENT"])

        self.tables['supplier'] = make_table("SUPPLIER", column_list=["SUPPKEY", "NAME", "ADDRESS", "NATIONKEY",
                                                                      "PHONE", "ACCTBAL", "COMMENT"])

        log.info("------------------------")
        log.info("Created TPCH Schema.")
        log.info("------------------------")


if __name__=="__main__":

    args = getargs()
    logging.basicConfig(filename=args.log, level=logging.INFO)
    config = read_config(args.config)

    tpch = TPCH()
    
    log.info("Accessing tables stored in: %s"%config.get("DATA", "PATH"))

    datapath = config.get("DATA", "PATH")
    fnames = [x for x in os.listdir(datapath) if x.endswith('.tbl')]

    for fname in fnames:
        table = fname.split('.tbl')[0]
        f = open('%s/%s' % (datapath, fname), 'r')
        for line in f:
            data = line.split('|')[:-1]
            tpch.tables[table].insert_list(data)

    tables = [(tpch.tables['customer'], tpch.tables['orders']), (tpch.tables['orders'], tpch.tables['lineitem'])]
    column_pairs = [('CUSTKEY', 'CUSTKEY'), ('ORDERKEY', 'ORDERKEY')]


    from algo1 import *
    num_samp = 2
    method = 'Extended-Olken'
    samps = sampler(num_samp, method, tables, column_pairs)
    # result = chain_join(tables, column_pairs, tbl_name=True)
    # print(len(result.data[result.data.keys()[0]]))
