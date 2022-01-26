from copy import copy, deepcopy
from typing import List

from cond_parser import CondParser
from conveyor import Conveyor
from expr_parser import ExprParser
from column import Column


class DataFrame:
    """
    Data structure containing tabular data, based on Conveyor.
    """
    def __init__(self, filename: str):
        """
        Initialize dataframe.
        :param filename: Name of the file to read the data from
        """
        self.__filename = filename
        self.__labels = None
        self.__conveyor = Conveyor(filename)

    def __iter__(self):
        return iter(self.__conveyor.run())

    def __getitem__(self, label):
        if type(label) == str:
            return Column(label, self.__filename)
        elif type(label) == list:
            df = DataFrame(self.__filename)
            df.__labels = label
            df.__conveyor.labels = label
            return df

    def __len__(self):
        return len(self.__conveyor)

    @property
    def columns(self):
        """
        The column labels of the dataframe.
        """
        if self.__labels == None:
            return self.__conveyor.keys
        else:
            return self.__labels

    @property
    def size(self):
        """
        Return an int representing the number of elements in this object.
        """
        return len(self) * len(self.columns)

    @property
    def shape(self):
        """
        Return a tuple representing the dimensionality of the DataFrame.
        """
        return len(self), len(self.columns)

    @property
    def empty(self):
        """
        Indicator whether DataFrame is empty.
        """
        return len(self) == 0

    def copy(self, deep: bool = True):
        """
        Make a copy of this object’s indices and data.
        :param deep: Make a deep copy, including a copy of the data and the indices
        :return: Copy of the dataframe
        """
        if deep:
            new_df = deepcopy(self)
        else:
            new_df = copy(self)
        return new_df

    def head(self, n: int = 5):
        """
        :param n: Number of rows to select
        :return: A new dataframe containing the first n rows
        """
        new_df = deepcopy(self)
        new_df.__conveyor.head(n)
        return new_df

    def transform(self, func, inplace: bool = False):
        """
        Call func on self producing a DataFrame with transformed values.
        :param func: Function or a list of functions to use for transforming the data
        :param inplace: If True, perform operation in-place
        :return: A dataframe with transformed values
        """
        if inplace:
            if isinstance(func, List):
                for f in func:
                    self.transform(f, True)
            else:
                f = ExprParser(func).expr
                self.__conveyor.apply(f)
            return self
        else:
            new_df = self.copy()
            if isinstance(func, List):
                for f in func:
                    new_df.transform(f, True)
            else:
                f = ExprParser(func).expr
                new_df.__conveyor.apply(f)
            return new_df

    def filter(self, cond, inplace: bool = True):
        """
        Select the rows that fit the condition
        :param cond: Condition to use for filtering
        :param inplace: If True, perform operation in-place
        :return: Filtered dataframe
        """
        if inplace:
            op = CondParser(cond).cond
            self.__conveyor.filter(op)
            return self
        else:
            new_df = self.copy()
            return new_df.filter(cond, True)

    def sort_values(self, by: str, ascending: bool = True, inplace: bool = False):
        """
        Sort dataframe by the values of a specified column.
        :param by: Name of the column to sort by
        :param ascending: Sort ascending vs. descending
        :param inplace: If True, perform operation in-place
        :return: DataFrame with sorted values
        """
        if inplace:
            self.__conveyor = self.__conveyor.sort(by, ascending)
            return self
        else:
            new_df = self.copy()
            new_df.__conveyor = new_df.__conveyor.sort(by, ascending)
            return new_df

    def quantile(self, q: float, sort_by: str):
        """
        :param q: Value between 0 <= q <= 1, the quantile to compute
        :param sort_by: Name of the column to sort by
        :return: Values at the given quantile over the specified column
        """
        if q < 0 or q > 1:
            raise ValueError('Quantile must be  0 <= q <= 1.')
        return self.__conveyor.procentile(sort_by, q)

    def median(self, sort_by: str):
        """
        :param sort_by: Name of the column to sort by
        :return: The median of the values over the specified column
        """
        return self.__conveyor.median(sort_by)

    def sum(self, column, progressbar=True):
        return self.__conveyor.sum(column, progressbar)

    def mean(self, column, progressbar=True):
        return self.__conveyor.mean(column, progressbar)

    def min(self, column, progressbar=True):
        return self.__conveyor.min(column, progressbar)

    def max(self, column, progressbar=True):
        return self.__conveyor.max(column, progressbar)

    def pearson(self, col_x, col_y, progressbar=True):
        return self.__conveyor.pearson(col_x, col_y, progressbar)

    def spearman(self, col_x, col_y, progressbar=True):
        return self.__conveyor.spearman(col_x, col_y, progressbar)

    def kendall(self, col_x, col_y, progressbar=True):
        return self.__conveyor.kendall(col_x, col_y, progressbar)