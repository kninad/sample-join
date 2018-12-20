import matplotlib.pyplot as plt
import hashlib
import bisect

class KolmogorovSmirnov:

    def __init__(self, full_join_length):
        self.J = full_join_length
        self.ranks = []

    def add_tuple(self, t):
        rank = self.__map_tuple_to_J(t)
        bisect.insort(self.ranks, rank)

    def get_largest_difference(self, plot=False, filename=None):
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
            self.__plot(x, y, filename=filename)
        return max_diff

    def __map_tuple_to_J(self, t):
        h = hashlib.sha256(str(t)).hexdigest()
        return int(int(h, 16) % self.J)

    def __plot(self, x, y, filename=None, expected_count=50):
        xe = [i / (expected_count * 1.0 / self.J) for i in range(expected_count)]
        ye = [i * (1.0 / expected_count) for i in range(expected_count)]
        plt.scatter(xe, ye, color='red', label='expected')
        plt.plot(x, y, '-b', label='exprimental')
        plt.legend(loc='upper left')
        plt.ylim(0, 1)
        plt.xlim(0, self.J)
        if filename is None:
            plt.show()
        else:
            plt.savefig(filename, bbox_inches='tight')
