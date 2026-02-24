import re
from pathlib import Path

import pytest
import xarray as xr
from _commons import DATASETS
from compliance_checker.suite import CheckSuite
from importlib_metadata import entry_points

from cc_plugin_cc6.cc6 import CORDEXCMIP6 as cc6

# Checks - functions vs descriptions
mip_checkdict = {
    "check_table_id": "Table ID",
    "check_drs_CV": "DRS (CV)",
    "check_drs_consistency": "DRS (consistency)",
    "check_variable": "Present variables",
    "check_required_global_attributes": "Required global attributes (Presence)",
    "check_required_global_attributes_CV": "Required global attributes (CV)",
    "check_missing_value": "Missing values",
    "check_time_continuity": "Time continuity (within file)",
    "check_time_bounds": "Time bounds continuity (within file)",
    "check_time_range": "Time range consistency",
    "check_version_date": "version_date (CMOR)",
    "check_creation_date": "creation_date (CMOR)",
    "check_variable_attributes": "Variable attributes (CV)",
}
cc6_checkdict = {
    "check_format": "File format",
    "check_compression": "Compression",
    "check_time_chunking": "File chunking",
    "check_time_range_AS": "Time range (Archive Specifications)",
    "check_calendar": "Calendar (Archive Specifications)",
    "check_time_units": "Time units (Archive Specifications)",
    "check_references": "references (Archive Specifications)",
    "check_version_realization_info": "version_realization_info (Archive Specifications)",
    "check_grid_desc": "grid (description - Archive Specifications)",
    "check_driving_attributes": "Driving attributes (Archive Specifications)",
    "check_domain_id": "domain_id (CV)",
    "check_institution": "institution (CV)",
    "check_version_realization": "version_realization (Archive Specifications)",
    "check_grid_mapping": "grid_mapping label (Archive Specifications)",
    "check_lon_value_range": "Longitude value range (Archive Specifications)",
    "check_lat_lon_bounds": "Presence of latitude and longitude bounds (Archive Specifications)",
    "check_horizontal_axes_bounds": "Presence of horizontal axes bounds (Archive Specifications)",
}
cc6_checkdict = mip_checkdict | cc6_checkdict

# Expected check failures
expected_failures = dict()
expected_failures["TAS_REMO"] = {
    "check_compression": {
        2: [
            "It is recommended that data should be compressed with a 'deflate level' of '1' and enabled 'shuffle' option."
            " The 'shuffle' option is disabled.",
        ],
    },
    "check_horizontal_axes_bounds": {
        2: [
            "It is recommended for the variables 'rlat' and 'rlon' or 'x' and 'y' to have bounds defined."
        ],
    },
    "check_grid_mapping": {
        3: [
            "The grid_mapping variable 'rotated_latitude_longitude' needs to include information regarding the shape and size of the Earth"
        ],
    },
    "check_variable_definition": {
        1: ["'tas:comment' needs to include the specified comment from the CMOR table"],
    },
}
expected_failures["FXOROG_REMO"] = {
    "check_compression": {
        2: [
            "It is recommended that data should be compressed with a 'deflate level' of '1' and enabled 'shuffle' option."
            " The 'shuffle' option is disabled.",
        ],
    },
    "check_horizontal_axes_bounds": {
        2: [
            "It is recommended for the variables 'rlat' and 'rlon' or 'x' and 'y' to have bounds defined."
        ],
    },
    "check_grid_mapping": {
        3: [
            "The grid_mapping variable 'rotated_latitude_longitude' needs to include information regarding the shape and size of the Earth"
        ],
    },
    "check_variable_definition": {
        1: [
            "'orog:comment' needs to include the specified comment from the CMOR table"
        ],
    },
}
# Checks that report multiple results (at various severity levels)
multi_result_checks = ["check_variable_definition"]


def test_cc6_basic(load_test_data):
    ifile = DATASETS["TAS_REMO"]
    ifile_fx = DATASETS["FXOROG_REMO"]
    cs = CheckSuite()
    cs.load_all_available_checkers()
    ds = cs.load_dataset(ifile)
    ds_fx = cs.load_dataset(ifile_fx)
    res = cs.run_all(ds, ["cc6"], skip_checks=[])
    res_fx = cs.run_all(ds_fx, ["cc6"], skip_checks=[])
    print(res)  # noqa
    print(res_fx)  # noqa


def test_cc6_time_checks_only(load_test_data, tmp_path):
    ifile = DATASETS["TAS_REMO"]
    ofile = tmp_path / Path(ifile).name
    ds = xr.open_dataset(ifile)
    del ds.attrs["frequency"]
    ds.to_netcdf(ofile)
    cs = CheckSuite(
        options={
            "cc6": {
                "time_checks_only": True,
                "write_consistency_output": tmp_path / "consistency.txt",
            }
        }
    )
    cs.load_all_available_checkers()
    ds = cs.load_dataset(ofile)
    res = cs.run_all(
        ds,
        ["cc6"],
        include_checks=[
            "check_time_continuity",
            "check_time_bounds",
            "check_time_range",
        ],
        skip_checks=[],
    )
    print(res)  # noqa
    assert res["cc6"][1] == {}, f"Errors occurred: {res['cc6'][1]}"
    for check_result in res["cc6"][0]:
        assert (
            len(set(check_result.value)) == 1
        ), f"Inconsistent check values: {check_result.value}"


@pytest.mark.xfail
def test_cc6_check_has_id(load_test_data, cc6_checks):
    c = getattr(cc6, cc6_checks)
    assert c.__name__.startswith("§")
    assert re.fullmatch(r"^[0-9]*\.[0-9]*$", c.__name__[1:].split()[0])


class TestCC6Checks:
    @pytest.fixture(scope="class")
    def cs(self):
        cs = CheckSuite()
        ep = entry_points(group="compliance_checker.suites", name="cc6")
        cs._load_checkers(ep)
        return cs

    def _run_check(self, cs, dataset, checks):
        ds = cs.load_dataset(DATASETS[dataset])
        res = cs.run_all(ds, ["cc6"], checks)

        # Check for errors
        assert res["cc6"][1] == {}, f"Errors occurred: {res['cc6'][1]}"

        # Check the number of checks
        assert (
            len(checks) == 1
        ), f"Expected 1 check, found {len(checks)}: {', '.join(checks)}"
        if checks[0] in multi_result_checks:
            assert (
                len(res["cc6"][0]) == 2
            ), f"Expected 2 checks, found {len(res['cc6'][0])}"
        else:
            assert (
                len(res["cc6"][0]) == 1
            ), f"Expected 1 check, found {len(res['cc6'][0])}"

        # Check result objects
        severity_encountered = []
        for check_result in res["cc6"][0]:

            # Get check severity
            severity = check_result.weight
            severity_encountered.append(severity)

            # Check the score results
            if (
                dataset in expected_failures
                and checks[0] in expected_failures[dataset]
                and severity in expected_failures[dataset][checks[0]]
            ):
                assert (
                    len(set(check_result.value)) == 2
                ), f"Inconsistent check values: {check_result.value}"
            else:
                assert (
                    len(set(check_result.value)) == 1
                ), f"Inconsistent check values: {check_result.value}"

            # Are there any expected check failures?
            if (
                dataset in expected_failures
                and len(expected_failures[dataset].keys()) > 0
            ):
                # If this specific check failed, was that expected?
                if (
                    checks[0] in expected_failures[dataset]
                    and severity in expected_failures[dataset][checks[0]]
                ):
                    substrs = expected_failures[dataset][checks[0]][severity]
                    if substrs:
                        for substr in substrs:
                            assert any(substr in msg for msg in check_result.msgs), (
                                "Expected check "
                                f"'{checks[0]}' to fail with all of \"{', '.join(substrs)}\" but got \"{', '.join(check_result.msgs)}\"."
                            )
                    else:
                        assert (
                            len(check_result.msgs) > 0
                        ), f"Expected check '{checks[0]}' to fail, but it passed."
                # Else, assert that no messages were returned (== the check did not fail)
                else:
                    assert check_result.msgs == [], (
                        "Expected no messages for check "
                        f"'{checks[0]}' but got: {', '.join(check_result.msgs)}"
                    )
        # Ensure all expected check severities were encountered
        if dataset in expected_failures and checks[0] in expected_failures[dataset]:
            assert all(
                [
                    si in severity_encountered
                    for si in expected_failures[dataset][checks[0]].keys()
                ]
            )

    @pytest.mark.parametrize(
        "dataset",
        [
            "TAS_REMO",
            "FXOROG_REMO",
        ],
    )
    def test_all_cc6_checks(self, cs, dataset, cc6_checks):
        self._run_check(cs, dataset, [cc6_checks])
