import exception


class MiniDB:
    """
        simple key-value storage supporting nested transactions
    """
    def __init__(self):
        self.transactions_storage = [(dict(), dict())]
        self.command_switcher = {
            "SET": self._set,
            "GET": self._get,
            "UNSET": self._unset,
            "COUNTS": self._count,
            "BEGIN": self._begin,
            "ROLLBACK": self._rollback,
            "COMMIT": self._commit,
            "END": self._end,
        }
        self._close = False

    def command_parser(self, command):
        parsed = command.split(" ")
        try:
            res = self.command_switcher[parsed[0]]
        except KeyError:
            raise exception.CommandNotFound(parsed[0])
        try:
            return res(*parsed[1:])
        except IndexError:
            raise exception.WrongArgument(parsed[0], parsed[1:])

    def _set(self, *args):
        if args[1] == "NULL":
            raise exception.ReservedValue
        storage, counter_storage = self.transactions_storage[-1][0], self.transactions_storage[-1][1]
        old_value = self._get(*args)
        if old_value != "NULL":
            try:
                counter_storage[old_value] -= 1
                if counter_storage[old_value] == 0:
                    del counter_storage[old_value]
            except KeyError:
                counter_storage[old_value] = -1
        storage[args[0]] = args[1]
        try:
            counter_storage[args[1]] += 1
        except KeyError:
            counter_storage[args[1]] = 1

    def _get(self, *args):
        stop = -(len(self.transactions_storage) + 1)
        for i in range(-1, stop, -1):
            try:
                return self.transactions_storage[i][0][args[0]]
            except KeyError:
                continue
        return "NULL"

    def _unset(self, *args):
        storage, counter_storage = self.transactions_storage[-1][0], self.transactions_storage[-1][1]
        if len(self.transactions_storage) == 1:
            val = storage.pop(args[0], "NULL")
        else:
            val = self._get(args[0])
            storage[args[0]] = "NULL"
        if val is not "NULL":
            try:
                counter_storage[val] -= 1
                if counter_storage[val] == 0:
                    del counter_storage[val]
            except KeyError:
                counter_storage[val] = -1

    def _count(self, *args):
        count = 0
        for tr in self.transactions_storage:
            count += tr[1].get(args[0], 0)
        return str(count)

    def _begin(self, *args):
        self.transactions_storage.append((dict(), dict()))

    def _rollback(self, *args):
        if len(self.transactions_storage) == 1:
            raise exception.RollbackBeforeBegin
        self.transactions_storage.pop()

    def _commit(self, *args):
        if len(self.transactions_storage) == 1:
            raise exception.CommitBeforeBegin
        stop = -(len(self.transactions_storage))
        commited_keys = set()
        storage, counter_storage = self.transactions_storage[0][0], self.transactions_storage[0][1]
        for i in range(-1, stop, -1):
            commited_data = self.transactions_storage.pop()[0]
            for k, v in commited_data.items():
                if k in commited_keys:
                    continue
                old_value = storage.get(k, None)
                if old_value:
                    counter_storage[old_value] -= 1
                    if counter_storage[old_value] == 0:
                        del counter_storage[old_value]
                if v == "NULL":
                    storage.pop(k, None)
                    continue
                storage[k] = v
                try:
                    counter_storage[v] += 1
                except KeyError:
                    counter_storage[v] = 1
                commited_keys.add(k)

    def _end(self, *args):
        self._close = True

    def get_close(self):
        return self._close
