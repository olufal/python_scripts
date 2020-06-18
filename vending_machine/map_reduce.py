from abc import ABC, abstractmethod
from collections import defaultdict


class Mapper(ABC):
    def _map(self, data):
        pass


class Reducer(ABC):
    def _reduce(self, kvs):
        pass


class MapReduce(Mapper, Reducer):
    def __init__(self, mapper, reducer):
        self.mapper = mapper()
        self.reducer = reducer()

    def runMR(self, data_parts):
        storage = defaultdict(list)
        for data_part in data_parts:
            for key, value in self.mapper._map(data_part):
                storage[key].append(value)
        return self.reducer._reduce(storage)


#####################################################################

data = ["Handshake lets universities and employers connect with a single click",
        "leading to more diverse, high-quality networking opportunities for students",
        "and employers. Because Handshake now connects over 300,000 unique employers",
        "from every industry and region, most schools see a 2-3x increase in relevant",
        "job opportunities within the first 6 months of switching to Handshake"
        ]


class WCMapper(Mapper):
    def _map(self, data):
        words = data.split()
        lst = [(word, 1) for word in words]
        return lst


class WCReducer(Reducer):
    def _reduce(self, kvs):
        counts = [(key, sum(value)) for key, value in kvs.items()]
        return counts


for x in MapReduce(WCMapper, WCReducer).runMR(data):
    print("'{}': {}".format(x[0], x[1]))