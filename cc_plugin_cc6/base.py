import json
import os
import re
from datetime import timedelta
from pathlib import Path

import cftime
import xarray as xr
from compliance_checker.base import BaseCheck, BaseNCCheck, Result

from cc_plugin_cc6 import __version__

from ._constants import deltdic

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
        self.debug = True
        self.dataset = dataset
        self.options = self.options
        self.xrds = xr.open_dataset(dataset.filepath(), decode_times=False)
        # Get path to the tables
        if self.inputs.get("tables", False):
            cc6cv = self.inputs["tables"]
        else:
            cc6cv = "./"
            # Download tables
            #   to be implemented
            # Environment variable
            # cc6cv = os.getenv("CORDEXCMIP6TABLESPATH", "./")
        # Identify table prefix and table names
        tables = [
            t
            for t in os.listdir(cc6cv)
            if os.path.isfile(os.path.join(cc6cv, t))
            and t.endswith(".json")
            and "example" not in t
        ]
        table_prefix = tables[0].split("_")[0]
        table_names = ["_".join(t.split("_")[1:]).split(".")[0] for t in tables]
        if not all([table_prefix + "_" + t + ".json" in tables for t in table_names]):
            raise ValueError(
                "CMOR tables do not follow the naming convention '<project_id>_<table_id>.json'."
            )
        # Read CV and coordinate tables
        self.CV = self._read_CV(cc6cv, table_prefix, "CV")["CV"]
        self.coords = self._read_CV(cc6cv, table_prefix, "coordinate")
        self.grids = self._read_CV(cc6cv, table_prefix, "grids")
        self.formulas = self._read_CV(cc6cv, table_prefix, "formula_terms")
        # Read variable tables (variable tables)
        self.CT = {}
        for table in table_names:
            if table in ["CV", "grids", "coordinate", "formula_terms"]:
                continue
            self.CT[table] = self._read_CV(cc6cv, table_prefix, table)
            if "variable_entry" not in self.CT[table]:
                raise KeyError(
                    f"CMOR table '{table}' does not contain the key 'variable_entry'."
                )
            if "Header" not in self.CT[table]:
                raise KeyError(
                    f"CMOR table '{table}' does not contain the key 'Header'."
                )
            for key in ["table_id"]:
                if key not in self.CT[table]["Header"]:
                    print(table, key)
                    raise KeyError(
                        f"CMOR table '{table}' misses the key '{key}' in the header information."
                    )
        # Compile varlist for quick reference
        varlist = list()
        for table in table_names:
            if table in ["CV", "grids", "coordinate", "formula_terms"]:
                continue
            varlist = varlist + list(self.CT[table]["variable_entry"].keys())
        varlist = set(varlist)
        # Map DRS building blocks to the filename, filepath and global attributes
        self._map_drs_blocks()
        # Identify variable name(s)
        var_ids = [v for v in varlist if v in list(dataset.variables.keys())]
        self.varname = var_ids
        # Identify table_id, requested frequency and cell_methods
        try:
            self.table_id = dataset.getncattr("table_id")
        except AttributeError:
            self.table_id = "unknown"
        self.frequency = self._get_frequency()
        # In case of unset table_id -
        #  in some projects (eg. CORDEX), the table_id is not required,
        #  since there is one table per frequency, so table_id = frequency.
        if self.table_id == "unknown":
            if len([key for key in self.CT.keys() if self.frequency in key]) == 1:
                self.table_id = self.frequency
        self.cell_methods = self._get_cell_methods()

    def _infer_frequency(self, timevar):
        try:
            time = xr.decode_cf(
                self.xrds[timevar].copy(deep=True), decode_times=True, use_cftime=True
            )
            return xr.infer_freq(time)
        except ValueError:
            return "unknown"

    def _get_attr(self, attr):
        try:
            return self.dataset.getncattr(attr)
        except AttributeError:
            return "unknown"

    def _get_frequency(self):
        """Returns the requested frequency."""
        # Use table_id if available
        if self.table_id != "unknown":
            if len(self.varname) > 0:
                try:
                    return self.CT[self.table_id]["variable_entry"][self.varname[0]][
                        "frequency"
                    ]
                except KeyError:
                    return self._get_attr("frequency")
            else:
                return "unknown"
        return self._get_attr("frequency")

    def _get_cell_methods(self):
        """Returns the requested cell methods."""
        if self.table_id != "unknown":
            if len(self.varname) > 0:
                try:
                    return self.CT[self.table_id]["variable_entry"][self.varname[0]][
                        "cell_methods"
                    ]
                except KeyError:
                    return "unknown"
        return "unknown"

    def _read_CV(self, path, table_prefix, table_name):
        """Reads the specified CV table."""
        cc6cv = Path(path, f"{table_prefix}_{table_name}.json")
        try:
            with open(cc6cv) as f:
                return json.load(f)
        except (FileNotFoundError, IsADirectoryError, PermissionError) as e:
            raise Exception(
                f"Could not find or open table '{table_prefix}_{table_name}.json' under path '{path}'."
            ) from e

    def _compare_CV_element(self, el, val):
        """Compares value of a CV entry to a given value."""
        # ########################################################################################
        # 5-6 Types of CV entries ('*' is the element that is the value for comparison):
        # 0 # value
        # 1 # key -> *list of values
        # 2 # key -> *list of length 1 (regex)
        # 3 # key -> *dict key -> value
        # 4 # key -> *dict key -> dict key -> *value
        # 5 # key -> *dict key -> dict key -> *list of values
        # 6 # key (source_id) -> *dict key -> dict key (license_info) -> dict key (id, license) -> value (CMIP6 only)
        # ########################################################################################
        # 0 (2nd+ level comparison) #
        if isinstance(el, str):
            if self.debug:
                print(val, "->0")
            return bool(re.fullmatch(el, str(val), flags=re.ASCII)), []
        # 1 and 2 #
        elif isinstance(el, type(list())):
            if self.debug:
                print(val, "->1 and 2")
            if val not in el:
                return (
                    any(
                        [
                            bool(re.fullmatch(eli, str(val), flags=re.ASCII))
                            for eli in el
                        ]
                    ),
                    [],
                )
            else:
                return True, []
        # 3 to 6 #
        elif isinstance(el, type(dict())):
            if self.debug:
                print(val, "->3 to 6")
            if val in el.keys():
                # 3 #
                if isinstance(el[val], str):
                    if self.debug:
                        print(val, "->3")
                    return True, []
                # 4 to 6 #
                elif isinstance(el[val], type(dict())):
                    if self.debug:
                        print(val, "->4 to 6")
                    return True, list(el[val].keys())
                else:
                    raise ValueError(
                        f"Unknown CV structure for element: {el} and value {val}."
                    )
            else:
                return False, []
        # (Yet) unknown
        else:
            raise ValueError(
                f"Unknown CV structure for element: {el} and value: {val}."
            )

    def _compare_CV(self, dic2comp, errmsg_prefix):
        """Compares dictionary of key-val pairs with CV."""
        checked = {key: False for key in dic2comp.keys()}
        messages = []
        for attr in dic2comp.keys():
            if self.debug:
                print(attr)
            if attr in self.CV:
                if self.debug:
                    print(attr, "1st level")
                errmsg = f"""{errmsg_prefix}'{attr}' does not comply with the CV: '{dic2comp[attr] if dic2comp[attr] else 'unset'}'."""
                checked[attr] = True
                test, attrs_lvl2 = self._compare_CV_element(
                    self.CV[attr], dic2comp[attr]
                )
                # If comparison fails
                if not test:
                    messages.append(errmsg)
                # If comparison could not be processed completely, as the CV element is another dictionary
                else:
                    for attr_lvl2 in attrs_lvl2:
                        if attr_lvl2 in dic2comp.keys():
                            if self.debug:
                                print(attr, "2nd level")
                            errmsg_lvl2 = f"""{errmsg_prefix}'{attr_lvl2}' does not comply with the CV: '{dic2comp[attr_lvl2] if dic2comp[attr_lvl2] else 'unset'}'."""
                            checked[attr_lvl2] = True
                            try:
                                test, attrs_lvl3 = self._compare_CV_element(
                                    self.CV[attr][dic2comp[attr]][attr_lvl2],
                                    dic2comp[attr_lvl2],
                                )
                            except ValueError:
                                raise ValueError(
                                    f"Unknown CV structure for element {attr} -> {self.CV[attr][dic2comp[attr]][attr_lvl2]} / {attr_lvl2} -> {dic2comp[attr_lvl2]}."
                                )
                            if not test:
                                messages.append(errmsg_lvl2)
                            else:
                                if len(attrs_lvl3) > 0:
                                    raise ValueError(
                                        f"Unknown CV structure for element {attr} -> {dic2comp[attr]} -> {attr_lvl2}."
                                    )
        return checked, messages

    def _map_drs_blocks(self):
        """Maps the file metadata, name and location to the DRS building blocks and required attributes."""
        try:
            drs_path_template = re.findall(
                r"<([^<>]*)\>", self.CV["DRS"]["directory_path_template"]
            )
            drs_filename_template = re.findall(
                r"<([^<>]*)\>", self.CV["DRS"]["filename_template"]
            )
            self.drs_suffix = ".".join(
                self.CV["DRS"]["filename_template"].split(".")[1:]
            )
        except KeyError:
            raise KeyError("The CV does not contain DRS information.")

        # Map DRS path elements
        self.drs_dir = {}
        fps = os.path.dirname(
            os.path.normpath(os.path.realpath(self.dataset.filepath()))
        ).split(os.sep)
        for i in range(-1, -len(drs_path_template) - 1, -1):
            try:
                self.drs_dir[drs_path_template[i]] = fps[i]
            except IndexError:
                self.drs_dir[drs_path_template[i]] = False

        # Map DRS filename elements
        self.drs_fn = {}
        fns = os.path.basename(self.dataset.filepath()).split(".")[0].split("_")
        for i in range(len(drs_filename_template)):
            try:
                self.drs_fn[drs_filename_template[i]] = fns[i]
            except IndexError:
                self.drs_fn[drs_filename_template[i]] = False

        # Map DRS global attributes
        self.drs_gatts = {}
        for gatt in self.CV["required_global_attributes"]:
            if gatt in drs_path_template or gatt in drs_filename_template:
                try:
                    self.drs_gatts[gatt] = self.dataset.getncattr(gatt)
                except AttributeError:
                    self.drs_gatts[gatt] = False

    def check_drs_CV(self, ds):
        """DRS building blocks in filename and path checked against CV"""
        desc = "DRS (CV)"
        level = BaseCheck.HIGH
        out_of = 3
        score = 0
        messages = []

        # File suffix
        suffix = ".".join(os.path.basename(ds.filepath()).split(".")[1:])
        if self.drs_suffix == suffix:
            score += 1
        else:
            messages.append(
                f"File suffix differs from expectation ('{self.drs_suffix}'): '{suffix}'."
            )

        # DRS path
        drs_dir_checked, drs_dir_messages = self._compare_CV(
            self.drs_dir, "DRS path building block "
        )
        if len(drs_dir_messages) == 0:
            score += 1
        else:
            messages.extend(drs_dir_messages)

        # DRS filename
        drs_fn_checked, drs_fn_messages = self._compare_CV(
            self.drs_fn, "DRS filename building block "
        )
        if len(drs_fn_messages) == 0:
            score += 1
        else:
            messages.extend(drs_fn_messages)

        # Unchecked DRS path building blocks
        unchecked = [key for key in self.drs_dir.keys() if not drs_dir_checked[key]]
        if len(unchecked) == 0:
            score += 1
        else:
            messages.append(
                f"""DRS path building blocks could not be checked: {', '.join("'{0}'".format(ukey) for ukey in unchecked)}."""
            )

        # Unchecked DRS filename building blocks
        unchecked = [key for key in self.drs_fn.keys() if not drs_fn_checked[key]]
        if len(unchecked) == 0:
            score += 1
        else:
            messages.append(
                f"""DRS filename building blocks could not be checked: {', '.join("'{0}'".format(ukey) for ukey in unchecked)}."""
            )

        return self.make_result(level, score, out_of, desc, messages)

    def check_drs_consistency(self, ds):
        """DRS building blocks in filename, path and global attributes checked for consistency"""
        desc = "DRS (consistency)"
        level = BaseCheck.HIGH
        out_of = 1
        score = 0
        messages = []

        # Union of all DRS building blocks
        blocks = sorted(
            list(
                set(self.drs_gatts.keys()).union(
                    set(self.drs_fn.keys()).union(set(self.drs_dir.keys()))
                )
            )
        )
        flaw = False
        # Check if the values for the DRS building blocks are consistent
        for att in blocks:
            atts = {
                "file path": self.drs_dir.get(att, False),
                "file name": self.drs_fn.get(att, False),
                "global attributes": self.drs_gatts.get(att, False),
            }
            if len({x for x in atts.values() if x}) > 1:
                messages.append(
                    f"""Value for DRS building block '{att}' is not consistent between {" and ".join(["'"+key+"'" for key in sorted(list(atts.keys())) if atts[key]])}: {" and ".join(["'"+atts[key]+"'" for key in sorted(list(atts.keys())) if atts[key]])}."""
                )
                flaw = True
        if not flaw:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

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

    # def check_time_completeness(self):
    #    pass

    # def check_time_range(self):
    #    pass

    def check_time_chunking(self, ds):
        """Checks if the chunking with respect to the time dimension is in accordance with CORDEX-CMIP6 Archive Specifications."""
        desc = "File chunking."
        level = BaseCheck.MEDIUM
        score = 0
        out_of = 1
        messages = []

        # Check if frequency is known and supported
        # Supported is the intersection of:
        #  CORDEX-CMIP6: fx, 1hr, day, mon
        #  deltdic.keys() - whatever frequencies are defined there
        if self.frequency in ["unknown", "fx"]:
            return self.make_result(level, out_of, out_of, desc, messages)
        if self.frequency not in deltdic.keys() or self.frequency not in [
            "1hr",
            "day",
            "mon",
        ]:
            messages.append(f"Frequency '{self.frequency}' not supported.")
            return self.make_result(level, score, out_of, desc, messages)

        # Get the time dimension, calendar and units
        try:
            time = self.xrds.cf["time"]
        except KeyError:
            messages.append("Coordinate variable 'time' not found in file.")
            return self.make_result(level, score, out_of, desc, messages)
        if "calendar" not in time.attrs:
            messages.append("'time' variable has no 'calendar' attribute.")
        if "units" not in time.attrs:
            messages.append("'time' variable has no 'units' attribute.")
        if len(messages) > 0:
            return self.make_result(level, score, out_of, desc, messages)

        # Get the first and last time values
        first_time = time[0].values
        last_time = time[-1].values

        # Convert the first and last time values to cftime.datetime objects
        first_time = cftime.num2date(
            first_time, calendar=time.calendar, units=time.units
        )
        last_time = cftime.num2date(last_time, calendar=time.calendar, units=time.units)

        # File chunks as requested by CORDEX-CMIP6
        if self.frequency == "mon":
            nyears = 10
        elif self.frequency == "day":
            nyears = 5
        # subdaily
        else:
            nyears = 1

        # Calculate the expected start and end dates of the year
        expected_start_date = cftime.datetime(
            first_time.year, 1, 1, 0, 0, 0, calendar=time.calendar
        )
        expected_end_date = cftime.datetime(
            last_time.year + nyears, 1, 1, 0, 0, 0, calendar=time.calendar
        )

        # Apply calendar- and frequency-dependent adjustments
        offset = 0
        if time.calendar == "360_day" and self.frequency == "mon":
            offset = timedelta(hours=12)

        # Modify expected start and end dates based on cell_methods and above offset
        if bool(re.fullmatch("^.*time: point.*$", self.cell_methods, flags=re.ASCII)):
            expected_end_date = expected_end_date - timedelta(
                seconds=deltdic[self.frequency] - offset - offset
            )
        elif bool(
            re.fullmatch(
                "^.*time: (maximum|minimum|mean|sum).*$",
                self.cell_methods,
                flags=re.ASCII,
            )
        ):
            expected_start_date += timedelta(
                seconds=deltdic[self.frequency] / 2.0 - offset
            )
            expected_end_date -= timedelta(
                seconds=deltdic[self.frequency] / 2.0 - offset
            )
        else:
            messages.append(f"Cannot interpret cell_methods '{self.cell_methods}'.")

        if len(messages) == 0:
            errmsg = (
                f"{'Unless for the last file of a timeseries ' if nyears>1 else ''}'{nyears}' full simulation year{' is' if nyears==1 else 's are'} "
                f"expected in the data file for frequency '{self.frequency}'."
            )
            # Check if the first time is equal to the expected start date
            if first_time != expected_start_date:
                messages.append(
                    f"The first timestep differs from expectation ('{expected_start_date}'): '{first_time}'. "
                    + errmsg
                )
            # Check if the last time is equal to the expected end date
            if last_time != expected_end_date:
                messages.append(
                    f"The last timestep differs from expectation ('{expected_end_date}'): '{last_time}'. "
                    + errmsg
                )
        if len(messages) == 0:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)
