from typing import Callable


class CondParser:
    """
    Condition parser
    """
    def __init__(self, cond):
        """
        Initialize the condition parser.
        :param cond: Condition (string or callable)
        """
        if isinstance(cond, str):
            self.cond = self.__str_to_cond(cond)
        elif isinstance(cond, Callable):
            self.cond = cond
        else:
            raise ValueError('Expected a valid condition or a callable function.')

    @staticmethod
    def __str_to_cond(cond_str):
        """
        Convert string into a callable function.
        :param cond_str: Condition string
        :return: Callable parsed from the string
        """
        x_name = "df"  # TODO: parse x_name from cond_str
        cond = cond_str.replace(x_name, "x", len(cond_str))

        def tmp_fun(x):
            return eval(cond)

        return tmp_fun
