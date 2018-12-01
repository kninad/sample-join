import ConfigParser
import os, logging
import argparse
from Table import make_table
import csv


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
        # TODO populate schema from the files with csv
    
    def create_db(self):
        self.part = self.p = make_table("PART", column_list=[
            "PARTKEY", "NAME", "MFGR","BRAND", "TYPE", "SIZE", "CONTAINER",
            "RETAILPRICE", "COMMENT"
        ])

        self.supplier = self.s = make_table("SUPPLIER", column_list=["SUPPKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL", "COMMENT"])
        self.partsupply = self.ps = make_table("PARTSUPPLY", column_list= ["PARTKEY","SUPPKEY","AVAILQTY", "SUPPLYCOST" "COMMENT"])

        self.nation = self.n = make_table("NATION", column_list=["NATIONKEY", "NAME", "REGIONKEY", "COMMENT"])

        self.region = self.r = make_table("REGION", column_list=["REGIONKEY",  "NAME","COMMENT"])

        self.lineitem = self.l = make_table("LINEITEM", column_list=["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER", "QUANTITY", "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS", "SHIPDATE", "COMMITDATE", "RECEIPTDATE", "SHIPINSTRUCT", "SHIPMODE", "COMMENT"])

        self.orders = self.o = make_table("ORDERS", column_list=["ORDERKEY", "CUSTKEY","ORDERSTATUS", "TOTALPRICE", "ORDERDATE", "ORDERPRIORITY", "CLERK", "SHIPPRIORITY", "COMMENT"])

        log.info("------------------------")
        log.info("Created TPCH Schema.")
        log.info("------------------------")


if __name__=="__main__":

    args = getargs()
    logging.basicConfig(filename=args.log, level=logging.INFO)
    config = read_config(args.config)

    tpch = TPCH()
    
    # log.info("Accessing tables stored in: %s"%config.get("DATA", "PATH"))

    # datapath = config.get("DATA", "PATH")
    # with open(os.path.join(datapath, "nation.tbl"), "rb") as csvfile:
    #     spamreader = csv.reader(csvfile, delimiter='|')
    #     for row in spamreader:
    
    #         row.pop() # last empty '' string.
    #         log.info(row)
            
            

