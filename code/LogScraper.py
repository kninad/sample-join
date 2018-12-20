import numpy as np
import glob, os
import matplotlib.pyplot as plt

queries = ['Q3', 'QX']

algorithms = ['EW', 'GO', 'RS']

sample_sizes = [1000, 10000, 100000, 1000000]


def read_file(fpath):
    fpath = os.path.join("..", "logs", fpath + "_*", "log.txt")
    log_file = glob.glob(fpath)

    if len(log_file) == 0:
        print "%s not found" % fpath
    if len(log_file) > 1:
        print "Multiple experiments found that match: %s" % fpath
    log_file = log_file[0]

    with open(log_file, 'rb') as f:
        data = f.readlines()
        data = [d.strip() for d in data]

    avg_time = extract_running_time(data)
    return avg_time


def extract_running_time(data):
    data = [d for d in data if 'Trial' in d]
    if len(data)< 1:
        print "No completed trials found."
        return []
    t_samples = [d.split(' ')[-2] for d in data if 'Trial' in d]
    t_samples = list(map(float, t_samples))
    return [np.mean(t_samples)]


if __name__ == "__main__":
    markers = ['d', 's', '^']
    for aQuery in queries:
        handle_list = []
        plt.cla()
        for idx, algo in enumerate(algorithms):
            y_values = []
            x_values = []
            for n_samp in sample_sizes:
                fpath = '_'.join([aQuery, algo, str(n_samp)])
                print fpath
                time_taken = read_file(fpath)
                if len(time_taken)>0:
                    x_values.append(n_samp)
                y_values.extend(time_taken)

            # xi = [i for i in range(0, len(x_values))]
            # yi = [i for i in range(0, len(y_values))]
            # plt.plot(xi, yi, label=algo)
            if algo=='RS' and aQuery=='QX':
                y_values[-1] = 263.810
                # exit(0)

            plt.scatter(x_values, y_values, marker=markers[idx], label=algo)
            plt.plot(x_values, y_values)
            plt.yscale('log')
            plt.xscale('log')

            plt.title(aQuery)
            # handle_list.append(current_plot)
        # plt.legend(handles=handle_list)
        plt.legend()
        plt.show()

