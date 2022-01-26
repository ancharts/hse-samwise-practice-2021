"""
Testing class
"""
from itertools import islice

from conveyor import Conveyor
from dataframe import DataFrame
import scipy.stats
import numpy as np


def main():
    # useful filenames:
    # ~/Downloads/data-4-structure-3.csv
    # ~/Downloads/data-4-structure-3.csv.zip
    # c = Conveyor('../data/data_split_files.zip')

    def test_run():
        print('----Running Test Run-----')
        c = Conveyor('../data/data400.csv')
        for r in islice(c.run(), 10):
            print(r['Музей'])

    def test_filter():
        print('----Running Test Filter-----')
        c = Conveyor('../data/data400.csv')
        for r in islice(c.filter(lambda x: 'Охинский' in x['Музей']).run(),
                        10):
            print(r['Музей'])

    def test_filter_bad():
        print('----Running Test Filter Bad-----')
        c = Conveyor('../data/data400.csv')
        for r in islice(c.filter(lambda x: 'Оинкий' in x['Музей']).run(), 10):
            print(r['Музей'])

    def test_apply():
        print('----Running Test Apply-----')
        c = Conveyor('../data/data400.csv')

        def get_museum_name(x):
            tmp: str = x['Музей']
            i1 = tmp.find('"')
            i2 = tmp.rfind('"')
            x['Музей'] = x['Музей'][i1 + 1:i2]

        operation = c.apply(get_museum_name).run()
        for r in islice(operation, 10):
            print(r['Музей'])

    def test_df_head():
        print('----Running Test DF Head-----')
        df = DataFrame('../data/data400.csv').head(10)
        assert len(df) == 10
        for x in df:
            print(x['Музей'])

    def test_df_transform():
        print('----Running Test DF Transform-----')

        def get_museum_name(x):
            tmp: str = x['Музей']
            i1 = tmp.find('"')
            i2 = tmp.rfind('"')
            x['Музей'] = x['Музей'][i1 + 1:i2]

        df = DataFrame('../data/data400.csv').head(10)
        df_transformed = df.transform([get_museum_name, "df['Музей'] = df['Музей'] + ', автор: ' + df['Автор']"])
        for x in df_transformed:
            print(x['Музей'])

    def test_df_filter():
        print('----Running Test DF Filter-----')

        def is_museum(x):
            return 'музей' in x['Музей'].lower()

        df = DataFrame('../data/data400.csv').head(10)
        df.filter("'государственное' in df['Музей'].lower()").filter(is_museum)
        for x in df:
            print(x['Музей'])

    def test_sort():
        print('----Running Test Sort-----')
        c = Conveyor('../data/data400.csv')
        c.sort('Музей')

    def test_median():
        print('----Running Test Median-----')
        c = Conveyor('../data/data400.csv')
        print(c.median('Количество составляющих')['Количество составляющих'])

    def test_sum():
        print('----Running Test Sum-----')
        c = Conveyor('../data/data400.csv')
        print(c.sum('Регистрационный номер Госкаталога', False))

    def test_min():
        print('----Running Test Min-----')
        c = Conveyor('../data/data400.csv')
        print(c.min('Регистрационный номер Госкаталога', False))

    def test_max():
        print('----Running Test Max-----')
        c = Conveyor('../data/data400.csv')
        print(c.max('Регистрационный номер Госкаталога', False))

    def test_df_sort():
        print('----Running Test DF Sort-----')

        def get_museum_name(x):
            tmp: str = x['Музей']
            i1 = tmp.find('"')
            i2 = tmp.rfind('"')
            x['Музей'] = x['Музей'][i1 + 1:i2]

        df = DataFrame('../data/data400.csv').transform(get_museum_name).head(15)
        for i, x in enumerate(df):
            print(i, x['Музей'])
        df_head_sorted = df.sort_values(by='Музей')
        for i, x in enumerate(df_head_sorted):
            print(i, x['Музей'])

    def test_df_quantile():
        print('----Running Test DF Quantile-----')

        df = DataFrame('../data/data400.csv').head(10)
        print('q=0.1', df.quantile(0.1, 'Музей')['Музей'])
        print('q=0.5', df.quantile(0.5, 'Музей')['Музей'])
        print('q=0.9', df.quantile(0.9, 'Музей')['Музей'])

        for i, x in enumerate(df.sort_values('Музей')):
            print(i, x['Музей'])

    def test_df_mean():
        print('----Running Test DF Mean-----')
        df = DataFrame('../data/data400.csv')
        print(df.mean('Регистрационный номер Госкаталога', False))

    def test_df_pearson():
        print('----Running Test DF Pearson-----')
        df = DataFrame('../data/data400.csv')
        print(df.pearson('Внутренний идентификатор', 'Регистрационный номер Госкаталога', False))

    def test_df_pearson_const_array():
        print('----Running Test DF Pearson Const Array-----')
        df = DataFrame('../data/data400.csv')
        print(df.pearson('Количество составляющих', 'Регистрационный номер Госкаталога', False))

    def test_df_spearman():
        print('----Running Test DF Spearman-----')
        df = DataFrame('../data/data400.csv')
        print(df.spearman('Внутренний идентификатор', 'Регистрационный номер Госкаталога', False))

    def test_df_kendall():
        print('----Running Test DF Kendall-----')
        df = DataFrame('../data/data400.csv')
        print(df.kendall('Внутренний идентификатор', 'Регистрационный номер Госкаталога', False))

    def test_column_getitem():
        print('----Running Test Column-----')
        df = DataFrame('../data/data400.csv')
        col = df['Наименование предмета']
        print(np.array(col)[:25])

    def test_scipy_pearson():
        print('----Running Test Scipy Pearson-----')
        df = DataFrame('../data/data400.csv')
        print(scipy.stats.pearsonr(df['Внутренний идентификатор'], df['Регистрационный номер Госкаталога']))

    def test_scipy_spearman():
        print('----Running Test Scipy Spearman-----')
        df = DataFrame('../data/data400.csv')
        print(scipy.stats.spearmanr(df['Внутренний идентификатор'], df['Регистрационный номер Госкаталога']))

    def test_scipy_kendall():
        print('----Running Test Scipy Kendall-----')
        df = DataFrame('../data/data400.csv')
        print(scipy.stats.kendalltau(df['Внутренний идентификатор'], df['Регистрационный номер Госкаталога']))

    def test_mem():
        from scipy.stats import pearsonr
        df = DataFrame('../data/SpotifyFeatures 2.csv')
        col1 = df['popularity']
        col2 = df['duration_ms']
        print(len(col1), len(col2))
        pearsonr(col1, col2)

    test_run()
    test_filter()
    test_filter_bad()
    test_apply()
    test_df_head()
    test_df_transform()
    test_df_filter()
    test_sort()
    test_median()

    print()

    test_sum()
    test_min()
    test_max()
    test_df_sort()
    test_df_quantile()
    test_df_mean()
    test_df_pearson()
    test_df_pearson_const_array()
    test_df_spearman()
    test_column_getitem()

    print()

    test_scipy_pearson()
    test_scipy_spearman()
    test_scipy_kendall()
    test_mem()

if __name__ == '__main__':
    main()
