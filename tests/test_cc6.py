import os

from compliance_checker.suite import CheckSuite


def test_cc6_basic():
    ifile = "tas_EUR-12_MPI-ESM1-2-LR_historical_r1i1p1f1_CLMcom-UDAG_ICON-CLM_v1-r1_1hr_201601010000-201601010000.nc"
    cs = CheckSuite()
    cs.load_all_available_checkers()
    if os.path.isfile(ifile):
        ds = cs.load_dataset(ifile)
        res = cs.run_all(ds, ["cc6"], skip_checks=[])
        print(res)  # noqa


def test_cc6_fail():
    ifile = "t_E-OBS_mon_195001.nc"
    cs = CheckSuite()
    cs.load_all_available_checkers()
    if os.path.isfile(ifile):
        ds = cs.load_dataset(ifile)
        res = cs.run_all(ds, ["cc6"], skip_checks=[])
        print(res)  # noqa
