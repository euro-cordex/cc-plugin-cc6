import json
import os
from pathlib import Path

from compliance_checker.base import BaseCheck, BaseNCCheck, Result

from cc_plugin_cc6 import __version__

# import xarray as xr
# import cf_xarray, cftime


class CORDEXCIMP6Base(BaseCheck):
    register_checker = True
    _cc_spec = "cc6"
    _cc_spec_version = "1.0"
    _cc_description = "Checks compliance with CORDEX-CMIP6."
    _cc_url = "https://github.com/euro-cordex/cc-plugin-cc6"
    _cc_checker_version = __version__
    _cc_display_headers = {3: "Required", 2: "Recommended", 1: "Suggested"}


class CORDEXCMIP6(BaseNCCheck, CORDEXCIMP6Base):
    register_checker = True

    @classmethod
    def make_result(cls, level, score, out_of, name, messages):
        return Result(level, (score, out_of), name, messages)

    def setup(self, dataset):
        self.dataset = dataset
        self.options = self.options
        # self.ds = xr.open_dataset(dataset.filepath(), decode_times=False)
        # Read CV and CMOR tables
        self.CV = self._read_CV("CV")["CV"]
        self.CT1hr = self._read_CV("1hr")
        self.CT6hr = self._read_CV("6hr")
        self.CTday = self._read_CV("day")
        self.CTmon = self._read_CV("mon")
        self.CTfx = self._read_CV("fx")
        self.coords = self._read_CV("coordinate")
        self.grids = self._read_CV("grids")
        self.formulas = self._read_CV("formula_terms")
        varlist = set(
            list(self.CT1hr["variable_entry"].keys())
            + list(self.CT6hr["variable_entry"].keys())
            + list(self.CTday["variable_entry"].keys())
            + list(self.CTmon["variable_entry"].keys())
            + list(self.CTfx["variable_entry"].keys())
        )
        # Identify variable name (cf_xarray)
        var_ids = [v for v in varlist if v in list(dataset.variables.keys())]
        self.varname = var_ids

    def _read_CV(self, table_name):
        """Reads the specified CV table."""
        if self.inputs.get("tables", False):
            cc6cv = self.inputs["tables"]
        else:
            cc6cv = os.getenv("CORDEXCMIP6TABLESPATH", "./")
        cc6cv = Path(cc6cv, f"CORDEX-CMIP6_{table_name}.json")
        # Read CV
        try:
            with open(cc6cv) as f:
                return json.load(f)
        except (FileNotFoundError, IsADirectoryError, PermissionError) as e:
            raise Exception(
                f"Could not find or open '{cc6cv}'. Please make sure the environment variable 'CORDEXCMIP6TABLESPATH' is set and points to the Tables subdirectory of the cloned 'cordex-cmip6-cmor-tables' git repository."
            ) from e

    def check_format(self, ds):
        """Checks if the file is in the expected format."""
        desc = "File format"
        level = BaseCheck.HIGH
        out_of = 1
        score = 0
        messages = []

        # Expected for raw model output
        disk_format_expected = "HDF5"
        data_model_expected = "NETCDF4"
        data_model_expected = "NETCDF4_CLASSIC"

        if (
            ds.disk_format != disk_format_expected
            or ds.data_model != data_model_expected
        ):
            messages.append(
                f"File format differs from expectation ({data_model_expected}/{disk_format_expected}): '{ds.data_model}/{ds.disk_format}'."
            )
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_variable(self, ds):
        """Checks if all variables in the file are part of the CV."""
        desc = "Present variables"
        level = BaseCheck.HIGH
        out_of = 1
        score = 0
        messages = []

        if len(self.varname) > 1:
            messages.append(
                f"More than one variable present in file: {', '.join(self.varname)}. Only the first one will be checked."
            )
        elif len(self.varname) == 0:
            messages.append("No requested variable could be identified in the file.")

        # Create list of all CV coordinates, grids, formula_terms
        cvars = []
        for entry in list(self.grids["axis_entry"].keys()):
            cvars.append(self.grids["axis_entry"][entry]["out_name"])
        for entry in list(self.grids["variable_entry"].keys()):
            cvars.append(self.grids["variable_entry"][entry]["out_name"])
        for entry in list(self.coords["axis_entry"].keys()):
            cvars.append(self.coords["axis_entry"][entry]["out_name"])
        for entry in list(self.formulas["formula_entry"].keys()):
            cvars.append(self.formulas["formula_entry"][entry]["out_name"])
        cvars = set(cvars)
        # Add bounds
        bounds = []
        for var in list(ds.variables.keys()):
            if var in cvars:
                bnd = getattr(ds.variables[var], "bounds", False)
                if bnd:
                    bounds.append(bnd)
        # Add grid_mapping
        if len(self.varname) > 0:
            crs = getattr(ds.variables[self.varname[0]], "grid_mapping", False)
            if crs:
                cvars |= {crs}
        # Identify unknown variables / coordinates
        unknown = []
        for var in list(ds.variables.keys()):
            if var not in cvars and var not in self.varname and var not in bounds:
                unknown.append(var)
        if len(unknown) > 0:
            messages.append(
                f"(Coordinate) variable(s) {', '.join(unknown)} is/are not part of the CV."
            )
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_compression(self, ds):
        """Checks if the main variable is compressed in the recommended way."""
        desc = "Compression"
        level = BaseCheck.MEDIUM
        out_of = 1
        score = 0
        messages = []

        if len(self.varname) > 0:
            varname = self.varname[0]
        else:
            varname = False
        if varname is False:
            score += 1
        elif (
            ds[varname].filters()["complevel"] != 1
            or ds[varname].filters()["shuffle"] is False
        ):
            messages.append(
                "It is recommended that data should be compressed with a 'deflate level' of '1' and enabled 'shuffle' option."
            )
            if ds[varname].filters()["complevel"] < 1:
                messages.append(" The data is uncompressed.")
            elif ds[varname].filters()["complevel"] > 1:
                messages.append(
                    " The data is compressed with a higher 'deflate level' than recommended, this can lead to performance issues when accessing the data."
                )
            if ds[varname].filters()["shuffle"] is False:
                messages.append(" The 'shuffle' option is disabled.")
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_required_global_attributes(self, ds):
        """Checks presence of mandatory global attributes."""
        desc = "Required global attributes."
        level = BaseCheck.HIGH
        score = 0
        messages = []

        required_attributes = self.CV.get("required_global_attributes", {})

        out_of = len(required_attributes)

        for attr in required_attributes:
            test = attr in list(self.dataset.ncattrs())
            score += int(test)
            if not test:
                messages.append(f"Required global attribute '{attr}' is missing.")

        return self.make_result(level, score, out_of, desc, messages)
