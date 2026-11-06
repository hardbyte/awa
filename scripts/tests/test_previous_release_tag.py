import importlib.util
from pathlib import Path
import unittest


spec = importlib.util.spec_from_file_location(
    "previous_release_tag", Path(__file__).parents[1] / "previous-release-tag.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
previous_release_tag = module.previous_release_tag


class PreviousReleaseTagTests(unittest.TestCase):
    def test_stable_predecessor_sorts_after_its_release_candidates(self):
        self.assertEqual(
            previous_release_tag("v0.7.1", ["v0.7.0-rc.1", "v0.7.0", "v0.7.1"]),
            "v0.7.0",
        )

    def test_rerun_ignores_newer_tags(self):
        self.assertEqual(
            previous_release_tag("v0.6.8", ["v0.6.7", "v0.6.8", "v0.6.9"]),
            "v0.6.7",
        )

    def test_prerelease_uses_the_same_minor_version(self):
        self.assertEqual(
            previous_release_tag("v0.7.1-rc.1", ["v0.7.0", "v0.7.1-rc.1"]),
            "v0.7.0",
        )

    def test_semver_prerelease_ordering(self):
        tags = [
            "v1.0.0-alpha", "v1.0.0-alpha.1", "v1.0.0-alpha.beta",
            "v1.0.0-beta", "v1.0.0-beta.2", "v1.0.0-beta.11",
            "v1.0.0-rc.1", "v1.0.0-rc.2", "v1.0.0-rc.10", "v1.0.0",
        ]
        for previous, current in zip(tags, tags[1:]):
            with self.subTest(current=current):
                self.assertEqual(previous_release_tag(current, list(reversed(tags))), previous)

    def test_build_metadata_does_not_change_precedence(self):
        self.assertEqual(
            previous_release_tag("v1.2.3+build.2", ["v1.2.2", "v1.2.3", "v1.2.3+build.1"]),
            "v1.2.2",
        )

    def test_first_tag_in_minor_has_no_predecessor(self):
        self.assertEqual(previous_release_tag("v1.2.0-alpha.1", ["v1.1.9", "v1.3.0"]), "")

    def test_invalid_tags_are_ignored(self):
        self.assertEqual(
            previous_release_tag("v1.2.3", ["other", "v1.2.02", "v1.2.3-01", "v1.2.2"]),
            "v1.2.2",
        )

    def test_current_tag_need_not_be_in_listing(self):
        self.assertEqual(previous_release_tag("v1.2.3", ["v1.2.2", "v1.2.4"]), "v1.2.2")

    def test_prefer_plain_tag_when_previous_version_has_build_tags(self):
        self.assertEqual(previous_release_tag("v1.2.3", ["v1.2.2+build.1", "v1.2.2"]), "v1.2.2")


if __name__ == "__main__":
    unittest.main()
