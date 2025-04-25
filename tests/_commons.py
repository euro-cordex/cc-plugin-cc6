import os

# Specify test data repo and cache directory
TEST_DATA_REPO_URL = "https://github.com/euro-cordex/py-cordex-data"
TEST_DATA_REPO_BRANCH = "main"
TEST_DATA_CACHE_DIR = os.path.expanduser("~/.cc6_testdata")

# Specify test datasets
TAS_REMO = os.path.join(
    TEST_DATA_CACHE_DIR,
    TEST_DATA_REPO_BRANCH,
    "CORDEX-CMIP6/DD/EUR-12/GERICS/ERA5/evaluation/r1i1p1f1/REMO2020-2-2/v1-r1/mon/tas/v20241120",
    "tas_EUR-12_ERA5_evaluation_r1i1p1f1_GERICS_REMO2020-2-2_v1-r1_mon_201901-202012.nc",
)
FXOROG_REMO = os.path.join(
    TEST_DATA_CACHE_DIR,
    TEST_DATA_REPO_BRANCH,
    "CORDEX-CMIP6/DD/EUR-12/GERICS/ERA5/evaluation/r1i1p1f1/REMO2020-2-2/v1-r1/fx/orog/v20241120",
    "orog_EUR-12_ERA5_evaluation_r1i1p1f1_GERICS_REMO2020-2-2_v1-r1_fx.nc",
)

DATASETS = {
    "TAS_REMO": TAS_REMO,
    "FXOROG_REMO": FXOROG_REMO,
}
