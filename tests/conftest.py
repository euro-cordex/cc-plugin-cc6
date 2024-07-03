import os
from collections import defaultdict

import pytest
from _commons import TEST_DATA_CACHE_DIR, TEST_DATA_REPO_BRANCH, TEST_DATA_REPO_URL
from compliance_checker.suite import CheckSuite
from git import Repo


# Fixture to load data repository used by the tests
#  Adapted from https://github.com/roocs/clisops/blob/master/tests/conftest.py
@pytest.fixture(scope="session", autouse=True)
def load_test_data():
    """
    This fixture ensures that the required test data repository
    has been cloned to the cache directory within the home directory.
    """
    target = os.path.join(TEST_DATA_CACHE_DIR, TEST_DATA_REPO_BRANCH)

    if not os.path.isdir(TEST_DATA_CACHE_DIR):
        os.makedirs(TEST_DATA_CACHE_DIR)

    if not os.path.isdir(target):
        repo = Repo.clone_from(TEST_DATA_REPO_URL, target)
        repo.git.checkout(TEST_DATA_REPO_BRANCH)
    else:
        repo = Repo(target)
        repo.git.checkout(TEST_DATA_REPO_BRANCH)
        repo.remotes[0].pull()


@pytest.fixture(scope="session")
def get_checkers_cc6():
    cs = CheckSuite()
    cs.load_all_available_checkers()
    checkers = cs._get_checks(cs.checkers["cc6"], [], defaultdict(lambda: None))
    return sorted([entry[0].__name__ for entry in checkers])


def pytest_generate_tests(metafunc):
    if metafunc.function.__name__ in ["test_all_cc6_checks", "test_cc6_check_has_id"]:
        metafunc.parametrize("cc6_checks", get_checkers_cc6.__wrapped__())
