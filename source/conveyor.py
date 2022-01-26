import csv
import io
import os
from sorted_in_disk.utils import read_iter_from_file
from sorted_in_disk import sorted_in_disk
from zipfile import ZipFile
from math import sqrt
import warnings
from tqdm import tqdm_notebook
from operation import FuncOperation, HeadOperation, ColumnOperation, StatOperation


# logging.basicConfig(encoding='utf-8', level=logging.DEBUG)


class Conveyor:
    def __init__(self, filename: str):
        """
        Initialize the conveyor class which allows conse
        :param filename: .zip or usual .csv file: works with any possible file
        """
        if not (filename.endswith('.csv') or filename.endswith('.zip')):
            pass  # todo
        self.filename = filename
        # https://docs.python.org/3/library/os.path.html#os.path.splitext
        # self.fileformat = os.path.splitext(filename)[1].replace('.', '')
        self.fileformat = filename.split('.')[-1]
        self.todos = []

        self.__keys = []
        self.__rows_cnt = 0
        self.not_computed = True
        self.labels = None
        self.__len = None

    def run(self):
        """
        Iteratively go through our conveyor of operations

        All the other methods configure this conveyor

        We assume that a zipfile has one .csv file which we parse

        Никаких параметров, только эвристики
        :return:
        """
        self.__rows_cnt = 0
        self.__keys = []

        def update(row):
            self.__rows_cnt += 1
            if not self.__keys:
                self.__keys = list(row.keys())

        if self.fileformat == 'csv':
            with open(self.filename, newline='') as csvfile:
                for r in self.rows_handler(csv.DictReader(csvfile)):
                    update(r)
                    if self.labels is not None:
                        res = []
                        for label in self.labels:
                            tmp = r[label]
                            if tmp.isnumeric():
                                res.append(float(r[label]))
                            else:
                                res.append(r[label])
                        yield res
                    else:
                        yield r
        elif self.fileformat == 'zip':
            # todo doesn't work if there is 1 directory with all files
            arh = ZipFile(self.filename)
            names = arh.namelist()
            for n in names:
                # files are not sorted here
                with arh.open(n, "r") as file:
                    with io.TextIOWrapper(file, encoding="utf-8") as f:
                        for r in self.rows_handler(csv.DictReader(f)):
                            update(r)
                            if self.labels is not None:
                                res = []
                                for label in self.labels:
                                    res.append(r[label])
                                yield res
                            else:
                                yield r
        else:
            pass
        # todo: recreate object before each test or reset todos after run
        self.todos = [task for task in self.todos if task.modifying]
        for task in self.todos:
            task.reset()
        self.not_computed = False

    def rows_handler(self, reader):
        for row in reader:
            is_good_row = True
            break_flag = False
            for task in self.todos:
                if task.op_type == 'filter':
                    op = task.func
                    is_good_row = is_good_row and op(row)
                elif task.op_type == 'apply':
                    op = task.func
                    op(row)
                elif task.op_type == 'head':
                    if is_good_row:
                        if task.cnt == task.n:
                            break_flag = True
                            break
                        task.cnt += 1
                elif task.op_type == 'sum':
                    task.cnt += float(row[task.column])
                elif task.op_type == 'pearson':
                    xi = float(row[task.col_x])
                    yi = float(row[task.col_y])
                    mean_x = task.values['mean_x']
                    mean_y = task.values['mean_y']
                    dx = xi - mean_x
                    dy = yi - mean_y
                    task.values['sum_dx_dy'] += dx * dy
                    task.values['sum_squares_dx'] += dx * dx
                    task.values['sum_squares_dy'] += dy * dy
                else:
                    pass

            if break_flag:
                break

            if is_good_row:
                yield row

    def filter(self, f):
        self.todos.append(FuncOperation('filter', func=f))
        self.not_computed = True
        return self

    def apply(self, f):
        self.todos.append(FuncOperation('apply', func=f))
        self.not_computed = True
        return self

    def head(self, n):
        self.todos.append(HeadOperation('head', n=n))
        self.not_computed = True
        return self

    def __len__(self):
        if self.not_computed:
            for _ in self.run():
                pass
        return self.__rows_cnt

    @property
    def keys(self):
        if self.not_computed:
            for _ in self.run():
                pass
        return self.__keys

    def sort(self, key, inplace=True):
        iterable_with_unsorted_data = read_iter_from_file(self.filename)
        label = key
        index = -1
        for keys_string in iterable_with_unsorted_data:
            keys = keys_string.split(',')
            for key in keys:
                index += 1
                if key == label:
                    break
            break

        if index == -1:
            return False

        def formatted(elem):
            if elem.isdigit():
                return float(elem)
            return elem

        sid = sorted_in_disk(iterable_with_unsorted_data, key=lambda line: formatted(list(csv.reader(line.splitlines()))[0][index]))

        if inplace:
            filename = self.filename
            with open(filename, 'w') as fout:
                print(keys_string, file=fout)
                for sorted_line in sid:
                    print(sorted_line, file=fout)
            return self
        else:
            filename = os.path.splitext(self.filename)[0] + '_sorted.csv'
            with open(filename, 'w') as fout:
                print(keys_string, file=fout)
                for sorted_line in sid:
                    print(sorted_line, file=fout)
            return Conveyor(filename)

    def procentile(self, key, procentile):
        sorted_conveyor = self.sort(key, False)
        num_rows = len(sorted_conveyor)
        percent_row_num = int(procentile * num_rows)
        for i, row in enumerate(sorted_conveyor.run()):
            if i + 1 == percent_row_num:
                return row

    def median(self, key):
        return self.procentile(key, 0.5)

    def sum(self, column, progressbar=True, method='sum'):
        total_rows = len(self)
        task_sum = ColumnOperation('sum', column, False)
        self.todos.append(task_sum)
        if progressbar:
            for _ in tqdm_notebook(self.run(), total=len(self), desc= "Searching {}".format(method)):
                pass
        else:
            for _ in self.run():
                pass
        return task_sum.cnt

    def mean(self, column, progressbar=True):
        return self.sum(column, progressbar, 'mean') / len(self)

    def min(self, column, progressbar=True):
        res = None
        if progressbar:
            for row in tqdm_notebook(self.run(), total=len(self), desc="Searching minimum"):
                if res is None:
                    res = row[column]
                else:
                    res = min(res, row[column])
        else:
            for row in self.run():
                if res is None:
                    res = row[column]
                else:
                    res = min(res, row[column])
        return res

    def max(self, column, progressbar=True):
        res = None
        if progressbar:
            for row in tqdm_notebook(self.run(), total=len(self),desc="Searching maximum"):
                if res is None:
                    res = row[column]
                else:
                    res = max(res, row[column])
        else:
            for row in self.run():
                if res is None:
                    res = row[column]
                else:
                    res = max(res, row[column])
        return res

    def pearson(self, col_x, col_y, progressbar=True):
        task_pearson = StatOperation('pearson', col_x, col_y, False)
        task_pearson.values = {'mean_x': self.mean(col_x, progressbar), 'mean_y': self.mean(col_y, progressbar), 'sum_dx_dy': 0,
                               'sum_squares_dx': 0, 'sum_squares_dy': 0}
        self.todos.append(task_pearson)
        if progressbar:
            for _ in tqdm_notebook(self.run(), total=len(self), desc="Pearson correlation"):
                pass
        else:
            for _ in self.run():
                pass
        if task_pearson.values['sum_squares_dx'] * task_pearson.values['sum_squares_dy'] == 0:
            warnings.warn("An input array is constant; the correlation coefficient is not defined.")
            return None
        task_pearson.coef = task_pearson.values['sum_dx_dy'] / sqrt(
            task_pearson.values['sum_squares_dx'] * task_pearson.values['sum_squares_dy'])
        return task_pearson.coef

    def ranking(self, col_x, col_y, progressbar=True):
        tmpfile = "tmp.csv"
        with open(self.filename, "r", newline='') as fin:
            with open(tmpfile, "w", newline='') as fout:
                reader = csv.reader(fin, lineterminator='\n')
                writer = csv.writer(fout)
                row = next(reader) + ['Index']
                writer.writerow(row)
                if progressbar:
                    for i, row in enumerate(tqdm_notebook(reader, total=len(self), desc="1. Ranking")):
                        writer.writerow(row + [str(i)])
                else:
                    for i, row in enumerate(reader):
                        writer.writerow(row + [str(i)])

        tmp_conveyor = Conveyor(tmpfile)
        tmp_conveyor.sort(col_x, False)

        with open('tmp_sorted.csv', "r", newline='') as fin:
            with open(tmpfile, "w", newline='') as fout:
                reader = csv.reader(fin, lineterminator='\n')
                writer = csv.writer(fout)
                row = next(reader) + ['Rank1']
                writer.writerow(row)
                if progressbar:
                    for i, row in enumerate(tqdm_notebook(reader, total=len(self),desc="2. Ranking")):
                        writer.writerow(row + [str(i)])
                else:
                    for i, row in enumerate(reader):
                        writer.writerow(row + [str(i)])

        tmp_conveyor = Conveyor(tmpfile)
        tmp_conveyor.sort(col_y, False)

        with open('tmp_sorted.csv', "r", newline='') as fin:
            with open(tmpfile, "w", newline='') as fout:
                reader = csv.reader(fin, lineterminator='\n')
                writer = csv.writer(fout)
                row = next(reader) + ['Rank2']
                writer.writerow(row)

                if progressbar:
                    for i, row in enumerate(tqdm_notebook(reader, total=len(self), desc="3. Ranking")):
                        writer.writerow(row + [str(i)])
                else:
                    for i, row in enumerate(reader):
                        writer.writerow(row + [str(i)])

        tmp_conveyor.sort('Index', False)
        return Conveyor('tmp_sorted.csv')

    def spearman(self, col_x, col_y, progressbar=True):
        tmp_conveyor = self.ranking(col_x, col_y, progressbar)
        sum_d = 0
        if progressbar:
            for row in tqdm_notebook(tmp_conveyor.run(), total=len(self), desc="Spearman correlation"):
                d = int(row['Rank1']) - int(row['Rank2'])
                sum_d += d * d
        else:
            for row in tmp_conveyor.run():
                d = int(row['Rank1']) - int(row['Rank2'])
                sum_d += d * d
        n = len(self)
        return 1 - 6 * sum_d / (n * (n * n - 1))

    def kendall(self, col_x, col_y, progressbar=True):
        num = 0
        if progressbar:
            for row_i in tqdm_notebook(self.run(), total=len(self), desc="Kendall correlation"):
                for row_j in self.run():
                    x_i = int(row_i[col_x])
                    y_i = int(row_i[col_y])
                    x_j = int(row_j[col_x])
                    y_j = int(row_j[col_y])
                    if (x_i - x_j) * (y_i - y_j) > 0:
                        num += 1
                    else:
                        num -= 1
        else:
            for row_i in self.run():
                for row_j in self.run():
                    x_i = int(row_i[col_x])
                    y_i = int(row_i[col_y])
                    x_j = int(row_j[col_x])
                    y_j = int(row_j[col_y])
                    if (x_i - x_j) * (y_i - y_j) > 0:
                        num += 1
                    else:
                        num -= 1
        n = len(self)
        return num / (n * (n - 1))