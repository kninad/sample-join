import hashlib
import bisect

class KolmogorovSmirnov:

    def __init__(self, full_join_length):
        self.J = full_join_length
        self.ranks = []

    def add_tuple(self, t):
        rank = self.__map_tuple_to_J(t)
        bisect.insort(self.ranks, rank)

    def get_largest_difference(self):
        max_diff = float('-inf')
        for i, rank in enumerate(self.ranks):
            diff = abs((i*1.0/len(self.ranks)) - (rank*1.0/self.J))
            max_diff = max(max_diff, diff)
        return diff

    def __map_tuple_to_J(self, t):
        h = hashlib.sha256(str(t)).hexdigest()
        return int(int(h, 16) % self.J)
