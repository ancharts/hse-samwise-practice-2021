from typing import Callable


class ExprParser:
    """
    Expression parser
    """
    def __init__(self, expr):
        """
        Initialize the expression parser.
        :param expr: Expression (string or callable)
        """
        if isinstance(expr, str):
            self.expr = self.__str_to_expr(expr)
        elif isinstance(expr, Callable):
            self.expr = expr
        else:
            raise ValueError('Expected a valid expression or a callable function.')

    @staticmethod
    def __str_to_expr(expr_str):
        """
        Convert string into a callable function.
        :param expr_str: Expression string
        :return: Callable parsed from the string
        """
        try:
            col, expr = expr_str.split(sep=' = ')
        except ValueError:
            raise ValueError('Error: Specifying transforming dataframe column expected. Please use  df[label] = ...  '
                             'notation.\nTransforming aborted.')
        x_name = col[:col.find('[')]
        if not len(x_name):
            raise ValueError('Error: Specifying dataframe name expected. Please use  df[label] = ...  notation.\n'
                  'Transforming aborted.')
        else:
            col = col.replace(x_name, "x", len(col))
            expr = expr.replace(x_name, "x", len(expr))

        if col.find("['") == -1 or col.find("']") == -1:
            raise ValueError('Error: Specifying transforming dataframe column expected. Please use  df[label] = ...  '
                             'notation.\nTransforming aborted.')
        col_label = col[col.find("['") + 2:col.find("']")]

        def tmp_fun(x):
            x[col_label] = eval(expr)

        return tmp_fun
