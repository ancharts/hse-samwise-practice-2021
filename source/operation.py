from abc import ABC
from typing import Callable


class Operation:
    """
    Base class representing an operation in Conveyor implementation.
    """
    op_type: str
    modifying: bool

    def __init__(self, op_type: str, modifying: bool, *args):
        raise NotImplementedError

    def reset(self):
        raise NotImplementedError


class FuncOperation(Operation, ABC):

    OP_TYPES_BASE_FUNC = {
        'filter': lambda x: True,
        'apply': lambda x: x,
    }

    def __init__(self, op_type: str, modifying: bool = True, func: Callable = None):
        """
        Initialize an operation operating with a function.
        :param op_type: Name of operation type
        :param modifying: True if operation modifies Conveyor, False otherwise
        :param func: Function the operation uses
        """
        if op_type not in self.OP_TYPES_BASE_FUNC.keys():
            raise ValueError(f'Unexpected operation type: {op_type}')
        self.op_type = op_type
        self.modifying = modifying
        if func is None:
            func = self.OP_TYPES_BASE_FUNC[self.op_type]
        self.func = func

    def reset(self):
        return


class HeadOperation(Operation, ABC):

    OP_TYPES = (
        'head',
    )

    def __init__(self, op_type: str, modifying: bool = True, n: int = 5):
        """
        Initialize an operation taking first N rows.
        :param op_type: Name of operation type
        :param modifying: True if operation modifies Conveyor, False otherwise
        :param n: Number of rows to take
        """
        if op_type not in self.OP_TYPES:
            raise ValueError(f'Unexpected operation type: {op_type}')
        self.op_type = op_type
        self.modifying = modifying
        self.n = n
        self.cnt = 0

    def reset(self):
        self.cnt = 0

class ColumnOperation(Operation, ABC):

    OP_TYPES = {
        'sum': lambda x: x,
    }

    def __init__(self, op_type: str, column: str, modifying: bool = True, func: Callable = None):
        """
        Initialize an operation operating with a function.
        :param op_type: Name of operation type
        :param modifying: True if operation modifies Conveyor, False otherwise
        :param func: Function the operation uses
        """
        if op_type not in self.OP_TYPES.keys():
            raise ValueError(f'Unexpected operation type: {op_type}')
        self.op_type = op_type
        self.column = column
        self.modifying = modifying
        if func is None:
            func = self.OP_TYPES[self.op_type]
        self.func = func
        self.cnt = 0

class StatOperation(Operation, ABC):

    OP_TYPES = {
        'pearson': lambda x: x,
    }

    def __init__(self, op_type: str, col_x: str, col_y: str, modifying: bool = True, func: Callable = None):
        """
        Initialize an operation operating with a function.
        :param op_type: Name of operation type
        :param modifying: True if operation modifies Conveyor, False otherwise
        :param func: Function the operation uses
        """
        if op_type not in self.OP_TYPES.keys():
            raise ValueError(f'Unexpected operation type: {op_type}')
        self.op_type = op_type
        self.col_x = col_x
        self.col_y = col_y
        self.modifying = modifying
        self.values = dict()
        if func is None:
            func = self.OP_TYPES[self.op_type]
        self.func = func
        self.coef = 0
