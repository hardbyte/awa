import importlib.util
from pathlib import Path
import unittest


spec = importlib.util.spec_from_file_location(
    "check_advisory_locks", Path(__file__).parents[1] / "check-advisory-locks.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

SAFE = """
CREATE FUNCTION s.f() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    BEGIN
        PERFORM pg_advisory_lock(42);
        PERFORM 1;
        PERFORM pg_advisory_unlock(42);
    EXCEPTION WHEN OTHERS OR query_canceled THEN
        PERFORM pg_advisory_unlock(42);
        RAISE;
    END;
END;
$$;
"""


def sql(text):
    return [message for _, _, message in module.sql_findings("m.sql", text)]


def rust(text, path="awa-model/src/x.rs"):
    return [message for _, _, message in module.rust_findings(path, text)]


class SqlTests(unittest.TestCase):
    def test_lock_released_on_cancel(self):
        self.assertEqual(sql(SAFE), [])

    def test_handler_misses_cancel(self):
        text = SAFE.replace("WHEN OTHERS OR query_canceled THEN", "WHEN OTHERS THEN")
        self.assertEqual(
            sql(text),
            ["session advisory lock: its block's EXCEPTION handler does not name query_canceled"],
        )

    def test_lock_taken_before_the_block_that_unlocks(self):
        text = SAFE.replace(
            "    BEGIN\n        PERFORM pg_advisory_lock(42);",
            "    PERFORM pg_advisory_lock(42);\n    BEGIN",
        )
        self.assertEqual(
            sql(text),
            ["session advisory lock: its block has no EXCEPTION handler that unlocks"],
        )

    def test_handler_does_not_unlock(self):
        text = SAFE.replace(
            "    EXCEPTION WHEN OTHERS OR query_canceled THEN\n        PERFORM pg_advisory_unlock(42);",
            "    EXCEPTION WHEN OTHERS OR query_canceled THEN\n        PERFORM 2;",
        )
        self.assertEqual(
            sql(text), ["session advisory lock: its block's query_canceled branch does not unlock"]
        )

    def test_each_branch_must_unlock(self):
        text = SAFE.replace(
            "    EXCEPTION WHEN OTHERS OR query_canceled THEN\n",
            "    EXCEPTION\n        WHEN query_canceled THEN\n            RAISE;\n        WHEN OTHERS THEN\n",
        )
        self.assertEqual(
            sql(text), ["session advisory lock: its block's query_canceled branch does not unlock"]
        )
        split = SAFE.replace(
            "    EXCEPTION WHEN OTHERS OR query_canceled THEN\n        PERFORM pg_advisory_unlock(42);\n        RAISE;\n",
            "    EXCEPTION\n        WHEN query_canceled THEN\n            PERFORM pg_advisory_unlock(42);\n            RAISE;\n"
            "        WHEN OTHERS THEN\n            PERFORM pg_advisory_unlock(42);\n            RAISE;\n",
        )
        self.assertEqual(sql(split), [])

    def test_a_specific_branch_before_others_must_unlock(self):
        # Postgres runs only the first matching branch, so the later cleanup
        # never runs for a division by zero.
        text = SAFE.replace(
            "    EXCEPTION WHEN OTHERS OR query_canceled THEN\n",
            "    EXCEPTION\n        WHEN division_by_zero THEN\n            RAISE;\n"
            "        WHEN OTHERS OR query_canceled THEN\n",
        )
        self.assertEqual(
            sql(text),
            ["session advisory lock: its block's division_by_zero branch does not unlock"],
        )

    def test_unlock_must_match_the_lock_form(self):
        shared = SAFE.replace("pg_advisory_lock(42)", "pg_advisory_lock_shared(42)")
        self.assertEqual(
            sql(shared),
            [
                "session advisory lock: its block's query_canceled branch does not unlock "
                "with pg_advisory_unlock_shared, which the lock's form needs"
            ],
        )
        self.assertEqual(sql(shared.replace("pg_advisory_unlock(", "pg_advisory_unlock_shared(")), [])
        self.assertEqual(sql(shared.replace("pg_advisory_unlock(", "pg_advisory_unlock_all(")), [])
        self.assertEqual(
            len(sql(shared.replace("pg_advisory_lock_shared(", "PG_ADVISORY_LOCK_SHARED("))),
            1,
            "the lock's form is read whatever its case",
        )
        exclusive = SAFE.replace("pg_advisory_unlock(", "pg_advisory_unlock_shared(")
        self.assertEqual(
            sql(exclusive),
            [
                "session advisory lock: its block's query_canceled branch does not unlock "
                "with pg_advisory_unlock, which the lock's form needs"
            ],
        )

    def test_case_arms_inside_a_branch_are_not_branches(self):
        text = SAFE.replace(
            "        PERFORM pg_advisory_unlock(42);\n        RAISE;\n    END;",
            "        PERFORM pg_advisory_unlock(42);\n        CASE\n            WHEN true THEN\n"
            "                RAISE;\n            ELSE\n                RAISE;\n        END CASE;\n    END;",
        )
        self.assertNotEqual(text, SAFE)
        self.assertEqual(sql(text), [])
        expression = SAFE.replace(
            "        PERFORM pg_advisory_unlock(42);\n        RAISE;\n    END;",
            "        PERFORM pg_advisory_unlock(42);\n        PERFORM CASE\n            WHEN true THEN 1\n"
            "            ELSE 2\n        END;\n        RAISE;\n    END;",
        )
        self.assertNotEqual(expression, SAFE)
        self.assertEqual(sql(expression), [])

    def test_every_lock_on_a_line_is_checked(self):
        text = SAFE.replace(
            "PERFORM pg_advisory_lock(42);",
            "PERFORM pg_advisory_lock(42); PERFORM pg_advisory_lock_shared(43);",
        )
        # The block's problem is reported against each lock it holds.
        self.assertEqual(
            sql(text),
            [
                "session advisory lock: its block's query_canceled branch does not unlock "
                "with pg_advisory_unlock_shared, which the lock's form needs"
            ]
            * 2,
        )

    def test_handler_must_cover_ordinary_errors(self):
        text = SAFE.replace("WHEN OTHERS OR query_canceled THEN", "WHEN query_canceled THEN")
        self.assertEqual(
            sql(text), ["session advisory lock: its block's EXCEPTION handler does not handle OTHERS"]
        )

    def test_try_and_shared_forms(self):
        for call in ("pg_try_advisory_lock(42)", "pg_advisory_lock_shared(42)"):
            text = SAFE.replace("pg_advisory_lock(42)", call).replace(
                "WHEN OTHERS OR query_canceled THEN", "WHEN OTHERS THEN"
            )
            self.assertEqual(len(sql(text)), 1, call)

    def test_transaction_locks_are_not_checked(self):
        self.assertEqual(sql("DO $$ BEGIN PERFORM pg_advisory_xact_lock(42); END $$;"), [])
        self.assertEqual(
            sql("DO $$\nBEGIN\n    PERFORM pg_advisory_xact_lock(42);\nEND;\n$$;"), []
        )

    def test_commented_out_and_marked_locks(self):
        self.assertEqual(sql("-- PERFORM pg_advisory_lock(42);"), [])
        self.assertEqual(
            sql("-- advisory-lock: released by the caller\nSELECT pg_advisory_lock(42);"), []
        )

    def test_end_if_and_end_loop_do_not_close_the_block(self):
        text = SAFE.replace(
            "        PERFORM 1;",
            "        IF true THEN\n            PERFORM 1;\n        END IF;\n"
            "        LOOP\n            EXIT;\n        END LOOP;",
        )
        self.assertEqual(sql(text), [])


class RustTests(unittest.TestCase):
    def test_raw_session_lock(self):
        self.assertEqual(len(rust('sqlx::query("SELECT pg_try_advisory_lock($1)")')), 1)

    def test_marked_dedicated_connection(self):
        text = '// advisory-lock: dedicated connection\nsqlx::query("SELECT pg_advisory_lock($1)")'
        self.assertEqual(rust(text), [])

    def test_guard_and_comments_are_exempt(self):
        text = 'sqlx::query("SELECT pg_try_advisory_lock($1)")'
        self.assertEqual(rust(text, module.RUST_GUARD), [])
        self.assertEqual(rust("// a session-scoped `pg_advisory_lock(` would leak"), [])

    def test_transaction_lock(self):
        self.assertEqual(rust('sqlx::query("SELECT pg_advisory_xact_lock($1)")'), [])


class RepositoryTests(unittest.TestCase):
    def test_repository_is_clean(self):
        self.assertEqual(module.check(Path(__file__).parents[2]), [])


if __name__ == "__main__":
    unittest.main()
