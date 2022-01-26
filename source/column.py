from conveyor import Conveyor
import numpy as np

class Column:

    def __init__(self, label: str, filename: str):
        self.MAX_RAM = 65536  # 0.5 * 1024 ** 3
        self.__label = label
        self.__filename = filename
        self.__conveyor = Conveyor(filename)
        self.__buffer = [''] * self.MAX_RAM
        self.__len = len(self.__conveyor)
        self.__cache = 0
        i = 0
        for r in self.__conveyor.run():
            if i >= self.MAX_RAM:
                break
            self.__buffer[i] = r[self.__label]
            i += 1

    def __getitem__(self, item):
        if item >= self.__len:
            raise IndexError("list index out of range")
        if item >= self.__cache and item < self.__cache + self.MAX_RAM:
            res = self.__buffer[item - self.__cache]
        else:
            self.__cache = item
            i = 0
            for r in self.__conveyor.run():
                if i - item >= self.MAX_RAM:
                    break
                if i == item:
                    res = r[self.__label]
                    self.__buffer[0] = res
                if i > item:
                    self.__buffer[i - item] = r[self.__label]
                i += 1
        if res.isnumeric():
            return np.float(res)
        return res

    def __len__(self):
        return self.__len

    def min(self, progressbar=True):
        return self.__conveyor.min(self.__label, progressbar)

    def max(self, progressbar=True):
        return self.__conveyor.max(self.__label, progressbar)

    def mean(self, progressbar=True):
        return self.__conveyor.mean(self.__label, progressbar)

    def sum(self, progressbar=True):
        return  self.__conveyor.sum(self.__label, progressbar)

    def corr(self, other, method = 'pearson', progressbar=True):
        if method=='pearson':
            return self.__conveyor.pearson(self.__label, other.__label, progressbar)
        if method == 'spearman':
            return self.__conveyor.spearman(self.__label, other.__label, progressbar)
        if method == 'kendall':
            return self.__conveyor.kendall(self.__label, other.__label, progressbar)
