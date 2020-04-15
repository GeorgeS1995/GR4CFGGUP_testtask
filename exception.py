class MiniDBException(Exception):
    pass


class CommandNotFound(MiniDBException):
    def __init__(self, err):
        self.err_output = err

    def __str__(self):
        return f"Command not found: {self.err_output}"


class WrongArgument(MiniDBException):
    def __init__(self, function, args):
        self.args = args
        self.function = function

    def __str__(self):
        return f"Wrong arguments {self.args} for command {self.function}"


class CommitBeforeBegin(MiniDBException):
    def __str__(self):
        return f"No transaction for commit"


class RollbackBeforeBegin(MiniDBException):
    def __str__(self):
        return f"No transaction for rollback"


class ReservedValue(MiniDBException):
    def __str__(self):
        return f'"NULL" is reserved value'
