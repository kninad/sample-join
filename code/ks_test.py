import matplotlib.pyplot as plt
import hashlib

class KolmogorovSmirnov:

    def __init__(self, full_join_length):
        self.J = full_join_length
        self.ranks = []

    def add_tuple(self, t):
        rank = self.__map_tuple_to_J(t)
        self.ranks.append(rank)

    def get_largest_difference(self, plot=False, filename=None, title=None):
        print('Running KS test')
        self.ranks = sorted(self.ranks)
        max_diff = float('-inf')
        x, y = [], []
        for i, rank in enumerate(self.ranks):
            M_p = (i * 1.0 / len(self.ranks))
            J_p = (rank * 1.0 / self.J)
            max_diff = max(max_diff, abs(M_p - J_p))
            if plot:
                x.append(rank)
                y.append(M_p)
        if plot:
            self.__plot(x, y, max_diff, filename=filename, title=title)
        return max_diff

    def __map_tuple_to_J(self, t):
        if type(t) != str:
            t = str(t)
        h = hashlib.sha256(t).hexdigest()
        return int(int(h, 16) % self.J)

    def __plot(self, x, y, d, filename=None, title=None, expected_count=50):
        xe = [i / (expected_count * 1.0 / self.J) for i in range(expected_count)]
        ye = [i * (1.0 / expected_count) for i in range(expected_count)]
        plt.scatter(xe, ye, color='red', label='expected')
        plt.plot(x, y, '-b', label='exprimental')
        plt.text(0.1 * self.J, 0.7, 'd = %.6f' % d)
        plt.legend(loc='upper left')
        plt.ylim(0, 1)
        plt.xlim(0, self.J)
        if title is not None:
            plt.title(title)
        if filename is None:
            plt.show()
        else:
            plt.savefig(filename, bbox_inches='tight')
        plt.close()
