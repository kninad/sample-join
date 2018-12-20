import logging
from Table import make_table

log = logging.getLogger(__name__)


class TPCH:

    def __init__(self, config):
        log.info("Initializing TPCH schema.")
        self.cfg = config
        self.tables = dict()
        self.create_db()
        # self.load_db()

    def create_db(self):
        self.tables['customer'] = make_table("CUSTOMER", column_list=["CUSTKEY", "NAME", "ADDRESS", "NATIONKEY",
                                                                      "PHONE", "ACCTBAL", "MKTSEGMENT", "COMMENT"],
                                             indexes=['CUSTKEY', 'NATIONKEY'])

        self.tables['lineitem'] = make_table("LINEITEM", column_list=["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER",
                                                                      "QUANTITY", "EXTENDEDPRICE", "DISCOUNT", "TAX",
                                                                      "RETURNFLAG", "LINESTATUS", "SHIPDATE",
                                                                      "COMMITDATE", "RECEIPTDATE", "SHIPINSTRUCT",
                                                                      "SHIPMODE", "COMMENT"],
                                             indexes=['ORDERKEY'])

        self.tables['nation'] = make_table("NATION", column_list=["NATIONKEY", "NAME", "REGIONKEY", "COMMENT"],
                                           indexes=['NATIONKEY', 'REGIONKEY'])

        self.tables['orders'] = make_table("ORDERS", column_list=["ORDERKEY", "CUSTKEY","ORDERSTATUS", "TOTALPRICE",
                                                                  "ORDERDATE", "ORDERPRIORITY", "CLERK", "SHIPPRIORITY",
                                                                  "COMMENT"],
                                           indexes=['ORDERKEY', 'CUSTKEY'])

        # self.tables['part'] = make_table("PART", column_list=["PARTKEY", "NAME", "MFGR","BRAND", "TYPE", "SIZE",
        #                                                       "CONTAINER", "RETAILPRICE", "COMMENT"])
        #
        # self.tables['partsupp'] = make_table("PARTSUPPLY", column_list= ["PARTKEY","SUPPKEY","AVAILQTY", "SUPPLYCOST",
        #                                                                  "COMMENT"])

        self.tables['region'] = make_table("REGION", column_list=["REGIONKEY",  "NAME","COMMENT"],
                                           indexes=['REGIONKEY'])

        self.tables['supplier'] = make_table("SUPPLIER", column_list=["SUPPKEY", "NAME", "ADDRESS", "NATIONKEY",
                                                                      "PHONE", "ACCTBAL", "COMMENT"],
                                             indexes=['NATIONKEY'])

        log.info("------------------------")
        log.info("Created TPCH Schema.")
        log.info("------------------------")


def get_schema(config):
    return TPCH(config)