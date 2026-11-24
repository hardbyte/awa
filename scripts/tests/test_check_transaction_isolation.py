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

    def test_application_owned_transaction_is_marked(self):
        text = "// isolation: the application owns this transaction\nlet tx = pool.begin().await?;"
        self.assertEqual(lines(text), [])
        text = "// isolation: owned\n\n\n\nlet tx = pool.begin().await?;"
        self.assertEqual(lines(text), [5])

    def test_comments_and_test_modules(self):
        self.assertEqual(lines("// pool.begin() would inherit the default"), [])
        text = "fn f() {}\n\n#[cfg(test)]\nmod tests {\n    let tx = pool.begin().await;\n}"
        self.assertEqual(lines(text), [])

    def test_code_after_a_test_module_is_checked(self):
        text = (
            "#[cfg(test)]\nmod tests {\n    fn f() { let tx = pool.begin(); }\n}\n\n"
            "fn g() {\n    let tx = pool.begin();\n}"
        )
        self.assertEqual(lines(text), [7])

    def test_out_of_line_test_module_hides_nothing(self):
        text = "#[cfg(test)]\nmod tests;\n\nfn g() {\n    let tx = pool.begin();\n}"
        self.assertEqual(lines(text), [5])

    def test_braces_in_literals_and_comments_do_not_end_the_module(self):
        text = (
            "#[cfg(test)]\nmod tests {\n"
            "    const A: &str = \"}\";\n"
            "    const B: &str = r#\"}}\"#;\n"
            "    const C: char = '}';\n"
            "    /* } */\n"
            "    fn f() { let tx = pool.begin(); }\n"
            "}\n\nfn g() {\n    let tx = pool.begin();\n}"
        )
        self.assertEqual(lines(text), [11])

    def test_begin_statements_name_their_isolation(self):
        self.assertEqual(lines('const B: &str = "BEGIN; SET LOCAL jit = off";'), [1])
        self.assertEqual(
            lines('const B: &str = "BEGIN ISOLATION LEVEL READ COMMITTED; SET LOCAL jit = off";'), []
        )
        self.assertEqual(lines('begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY")'), [])
        self.assertEqual(lines('begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")'), [])

    def test_begin_statements_that_write_run_at_read_committed(self):
        self.assertEqual(lines('begin_with("BEGIN ISOLATION LEVEL SERIALIZABLE")'), [1])
        self.assertEqual(lines('begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")'), [1])
        self.assertEqual(lines('begin_with("begin isolation level read committed")'), [])

    def test_repository_is_clean(self):
        self.assertEqual(module.check(Path(__file__).parents[2]), [])


if __name__ == "__main__":
    unittest.main()
