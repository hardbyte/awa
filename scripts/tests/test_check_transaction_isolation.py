import importlib.util
from pathlib import Path
import unittest


spec = importlib.util.spec_from_file_location(
    "check_transaction_isolation", Path(__file__).parents[1] / "check-transaction-isolation.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def lines(text):
    return [line for _, line, _ in module.findings("x.rs", text)]


class TransactionIsolationTests(unittest.TestCase):
    def test_pool_begin(self):
        self.assertEqual(lines("fn f() {\n    let tx = pool.begin().await?;\n}"), [2])
        self.assertEqual(lines("let tx = self.pool.try_begin().await?;"), [1])
        self.assertEqual(lines("let tx = self\n    .pool\n    .begin()\n    .await?;"), [2])

    def test_read_committed_and_other_receivers(self):
        self.assertEqual(lines("let tx = pool.begin_read_committed().await?;"), [])
        self.assertEqual(lines("let savepoint = tx.begin().await?;"), [])
        self.assertEqual(lines("let tx = conn.begin().await?;"), [])

    def test_comments_and_test_modules(self):
        self.assertEqual(lines("// pool.begin() would inherit the default"), [])
        text = "fn f() {}\n\n#[cfg(test)]\nmod tests {\n    let tx = pool.begin().await;\n}"
        self.assertEqual(lines(text), [])

    def test_repository_is_clean(self):
        self.assertEqual(module.check(Path(__file__).parents[2]), [])


if __name__ == "__main__":
    unittest.main()
