import re

import pytest
from _commons import FXOROG_REMO, TAS_REMO
from compliance_checker.suite import CheckSuite

from cc_plugin_cc6.cc6 import CORDEXCMIP6 as cc6


def test_cc6_basic(load_test_data):
    ifile = TAS_REMO
    ifile_fx = FXOROG_REMO
    cs = CheckSuite()
    cs.load_all_available_checkers()
    ds = cs.load_dataset(ifile)
    ds_fx = cs.load_dataset(ifile_fx)
    res = cs.run_all(ds, ["cc6"], skip_checks=[])
    res_fx = cs.run_all(ds_fx, ["cc6"], skip_checks=[])
    print(res)  # noqa
    print(res_fx)  # noqa


@pytest.mark.xfail
def test_all_cc6_checks(load_test_data, cc6_checks):
    cs = CheckSuite()
    cs._load_checkers([cc6])
    ds = cs.load_dataset(TAS_REMO)
    # Run a single check
    res = cs.run_all(ds, ["cc6"], [cc6_checks])

    # Assert that no errors occured
    assert res["cc6"][1] == {}
    # Assert that only one check was run
    assert len(res["cc6"][0]) == 1
    # Assert that the check yielded the expected result (score == out_of)
    assert len(set(res["cc6"][0][0].value)) == 1
    # Assert that no messages were returned
    assert res["cc6"][0][0].msgs == []


@pytest.mark.xfail
def test_cc6_check_has_id(load_test_data, cc6_checks):
    c = getattr(cc6, cc6_checks)
    assert c.__name__.startswith("§")
    assert re.fullmatch(r"^[0-9]*\.[0-9]*$", c.__name__[1:].split()[0])
