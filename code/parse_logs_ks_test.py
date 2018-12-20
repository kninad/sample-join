import ConfigParser
from ks_test import KolmogorovSmirnov
import os

def read_config(configpath='config.ini'):
    config = ConfigParser.RawConfigParser()
    config.read(configpath)
    return config

config = read_config()
logpath = config.get("LOGS", "PATH")
outpath = config.get("GRAPHS", "PATH")

queries, weights = ['QX'], ['GO', 'EW', 'RS']
J = 2400301184
for folder_name in os.listdir(logpath):
    if folder_name[:2] in queries and folder_name[3:5] in weights:
        logfile = open(os.path.join(logpath, folder_name, 'log.txt'), 'r')
        print('Working on %s' % folder_name)
        outfile = os.path.join(outpath, folder_name + '.png')
        KS = KolmogorovSmirnov(J)
        for line in logfile:
            if 'Sample:' in line:
                t = line.split('Sample: ')[1].split('\n')[0]
                KS.add_tuple(t)
        d = KS.get_largest_difference(plot=True, filename=outfile, title=folder_name)
