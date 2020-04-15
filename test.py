import unittest
import minidb
import exception


class MiniDBTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.db = minidb.MiniDB()
        self.commands = ("SET", "GET", "UNSET", "COUNT", "BEGIN", "ROLLBACK", "COMMIT")

    def test_allowed_comands(self):
        self.assertRaises(exception.CommandNotFound, self.db.command_parser, "wrong command")

    def test_reserved_name(self):
        self.assertRaises(exception.ReservedValue, self.db.command_parser, "SET a NULL")

    def test_wrong_args(self):
        self.assertRaises(exception.WrongArgument, self.db.command_parser, "SET a")

    def test_set_get(self):
        self.db.command_parser("SET a 10")
        self.assertEqual("10", self.db.command_parser("GET a"))

    def test_set_update_get(self):
        self.db.command_parser("SET a 10")
        self.assertEqual("10", self.db.command_parser("GET a"))
        self.db.command_parser("SET a 20")
        self.assertEqual("20", self.db.command_parser("GET a"))

    def test_unset(self):
        self.db.command_parser("SET a 10")
        self.assertEqual("10", self.db.command_parser("GET a"))
        self.assertEqual("1", self.db.command_parser("COUNTS 10"))
        self.db.command_parser("UNSET a")
        self.assertEqual("NULL", self.db.command_parser("GET a"))
        self.assertEqual("0", self.db.command_parser("COUNTS 10"))

    def test_empty_unset(self):
        self.db.command_parser("SET a 10")
        self.assertEqual("10", self.db.command_parser("GET a"))
        self.assertEqual("1", self.db.command_parser("COUNTS 10"))
        for _ in range(2):
            self.db.command_parser("UNSET a")
            self.assertEqual("NULL", self.db.command_parser("GET a"))
            self.assertEqual("0", self.db.command_parser("COUNTS 10"))

    def test_counts(self):
        for i in range(100):
            self.db.command_parser(f"SET {i} 10")
        self.assertEqual("100", self.db.command_parser("COUNTS 10"))

    def test_empty_commit(self):
        self.assertRaises(exception.CommitBeforeBegin, self.db.command_parser, "COMMIT")

    def test_empty_rollback(self):
        self.assertRaises(exception.RollbackBeforeBegin, self.db.command_parser, "ROLLBACK")

    def test_simple_transaction_commit(self):
        self.db.command_parser("SET a 10")
        self.assertEqual("1", self.db.command_parser("COUNTS 10"))
        self.assertEqual("0", self.db.command_parser("COUNTS 20"))
        self.db.command_parser("BEGIN")
        self.db.command_parser("SET a 20")
        self.assertEqual("0", self.db.command_parser("COUNTS 10"))
        self.assertEqual("1", self.db.command_parser("COUNTS 20"))
        self.db.command_parser("COMMIT")
        self.assertEqual("20", self.db.command_parser("GET a"))
        self.assertEqual("0", self.db.command_parser("COUNTS 10"))
        self.assertEqual("1", self.db.command_parser("COUNTS 20"))

    def test_simple_transaction_rollback(self):
        self.db.command_parser("SET a 10")
        self.assertEqual("1", self.db.command_parser("COUNTS 10"))
        self.assertEqual("0", self.db.command_parser("COUNTS 20"))
        self.db.command_parser("BEGIN")
        self.db.command_parser("SET a 20")
        self.assertEqual("0", self.db.command_parser("COUNTS 10"))
        self.assertEqual("1", self.db.command_parser("COUNTS 20"))
        self.db.command_parser("ROLLBACK")
        self.assertEqual("10", self.db.command_parser("GET a"))
        self.assertEqual("1", self.db.command_parser("COUNTS 10"))
        self.assertEqual("0", self.db.command_parser("COUNTS 20"))

    def test_unset_in_simple_transaction(self):
        self.db.command_parser("SET a 10")
        self.assertEqual("1", self.db.command_parser("COUNTS 10"))
        self.db.command_parser("BEGIN")
        self.db.command_parser("UNSET a")
        self.assertEqual("NULL", self.db.command_parser("GET a"))
        self.assertEqual("0", self.db.command_parser("COUNTS 10"))
        self.db.command_parser("ROLLBACK")
        self.assertEqual("10", self.db.command_parser("GET a"))
        self.assertEqual("1", self.db.command_parser("COUNTS 10"))

    def test_nested_transactions_commit(self):
        self.db.command_parser("SET a 10")
        self.db.command_parser("SET b 10")
        self.assertEqual("2", self.db.command_parser("COUNTS 10"))
        self.assertEqual("0", self.db.command_parser("COUNTS 20"))
        self.db.command_parser("BEGIN")
        self.db.command_parser("SET a 20")
        self.assertEqual("1", self.db.command_parser("COUNTS 10"))
        self.assertEqual("1", self.db.command_parser("COUNTS 20"))
        self.db.command_parser("BEGIN")
        self.db.command_parser("SET b 20")
        self.assertEqual("0", self.db.command_parser("COUNTS 10"))
        self.assertEqual("2", self.db.command_parser("COUNTS 20"))
        self.db.command_parser("COMMIT")
        self.assertEqual("20", self.db.command_parser("GET a"))
        self.assertEqual("20", self.db.command_parser("GET b"))
        self.assertEqual("0", self.db.command_parser("COUNTS 10"))
        self.assertEqual("2", self.db.command_parser("COUNTS 20"))

    def test_nested_transactions_rollback(self):
        self.db.command_parser("SET a 10")
        self.db.command_parser("SET b 10")
        self.assertEqual("2", self.db.command_parser("COUNTS 10"))
        self.assertEqual("0", self.db.command_parser("COUNTS 20"))
        self.db.command_parser("BEGIN")
        self.db.command_parser("SET a 20")
        self.assertEqual("1", self.db.command_parser("COUNTS 10"))
        self.assertEqual("1", self.db.command_parser("COUNTS 20"))
        self.db.command_parser("BEGIN")
        self.db.command_parser("SET b 20")
        self.assertEqual("0", self.db.command_parser("COUNTS 10"))
        self.assertEqual("2", self.db.command_parser("COUNTS 20"))
        # back to first transaction
        self.db.command_parser("ROLLBACK")
        self.db.command_parser("COMMIT")
        self.assertEqual("20", self.db.command_parser("GET a"))
        self.assertEqual("10", self.db.command_parser("GET b"))
        self.assertEqual("1", self.db.command_parser("COUNTS 10"))
        self.assertEqual("1", self.db.command_parser("COUNTS 20"))

    def test_unset_in_nested_transactions(self):
        self.db.command_parser("SET a 10")
        self.db.command_parser("SET b 10")
        self.assertEqual("2", self.db.command_parser("COUNTS 10"))
        self.assertEqual("0", self.db.command_parser("COUNTS 20"))
        self.db.command_parser("BEGIN")
        self.db.command_parser("SET a 20")
        self.assertEqual("1", self.db.command_parser("COUNTS 10"))
        self.assertEqual("1", self.db.command_parser("COUNTS 20"))
        self.db.command_parser("BEGIN")
        self.db.command_parser("UNSET b")
        self.assertEqual("0", self.db.command_parser("COUNTS 10"))
        self.assertEqual("1", self.db.command_parser("COUNTS 20"))
        # back to first transaction
        self.db.command_parser("ROLLBACK")
        self.db.command_parser("COMMIT")
        self.assertEqual("20", self.db.command_parser("GET a"))
        self.assertEqual("10", self.db.command_parser("GET b"))
        self.assertEqual("1", self.db.command_parser("COUNTS 10"))
        self.assertEqual("1", self.db.command_parser("COUNTS 20"))

    def test_a_lot_nested_transactions(self):
        for i in range(100):
            self.db.command_parser(f"SET {i} 10")
            self.db.command_parser("BEGIN")
        self.db.command_parser("COMMIT")
        for i in range(100):
            self.assertEqual("10", self.db.command_parser(f"GET {i}"))
        self.assertEqual("100", self.db.command_parser("COUNTS 10"))


if __name__ == '__main__':
    unittest.main()
