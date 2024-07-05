import json
import os
import re
from collections import ChainMap
from pathlib import Path

import cf_xarray  # noqa
import cftime
import numpy as np
import xarray as xr
from compliance_checker.base import BaseCheck, BaseNCCheck, Result

from cc_plugin_cc6 import __version__

from ._constants import deltdic

get_tseconds = lambda t: t.total_seconds()  # noqa
get_tseconds_vector = np.vectorize(get_tseconds)


def printtimedelta(d):
    """Return timedelta (s) as either min, hours, days, whatever fits best."""
    if d > 86000:
        return f"{d/86400.} days"
    if d > 3500:
        return f"{d/3600.} hours"
    if d > 50:
        return f"{d/60.} minutes"
    else:
        return f"{d} seconds"


class MIPCVCheckBase(BaseCheck):
    register_checker = False
    _cc_spec = ""
    _cc_spec_version = __version__
    _cc_description = "Checks compliance with given CV tables."
    _cc_checker_version = __version__
    _cc_display_headers = {3: "Required", 2: "Recommended", 1: "Suggested"}


class MIPCVCheck(BaseNCCheck, MIPCVCheckBase):
    register_checker = False

    @classmethod
    def make_result(cls, level, score, out_of, name, messages):
        return Result(level, (score, out_of), name, messages)

    def setup(self, dataset):
        self.debug = True
        self.dataset = dataset
        self.options = self.options
        self.filepath = os.path.realpath(
            os.path.normpath(os.path.expanduser(self.dataset.filepath()))
        )
        self.xrds = xr.open_dataset(
            self.filepath, decode_coords=True, decode_times=False
        )
        # Get path to the tables
        if self.inputs.get("tables", False):
            tables_path = self.inputs["tables"]
            self._initialize_CV_info(tables_path)
            self._initialize_time_info()
            self._initialize_coords_info()

        # Specify the global attributes that will be checked by a specific check
        #  rather than a general check against the value given in the CV
        #  (i.e. because it does not explicitly defined in the CV)
        self.global_attrs_hard_checks = ["variable_id", "time_range", "version"]

    def _initialize_CV_info(self, tables_path):
        """Find and read CV and CMOR tables and extract basic information."""
        # Identify table prefix and table names
        tables_path = os.path.normpath(
            os.path.realpath(os.path.expanduser(tables_path))
        )
        tables = [
            t
            for t in os.listdir(tables_path)
            if os.path.isfile(os.path.join(tables_path, t))
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
        self.CV = self._read_CV(tables_path, table_prefix, "CV")["CV"]
        self.CTcoords = self._read_CV(tables_path, table_prefix, "coordinate")
        self.CTgrids = self._read_CV(tables_path, table_prefix, "grids")
        self.CTformulas = self._read_CV(tables_path, table_prefix, "formula_terms")
        # Read variable tables (variable tables)
        self.CT = {}
        for table in table_names:
            if table in ["CV", "grids", "coordinate", "formula_terms"]:
                continue
            self.CT[table] = self._read_CV(tables_path, table_prefix, table)
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
        var_ids = [v for v in varlist if v in list(self.dataset.variables.keys())]
        self.varname = var_ids
        # Identify table_id, requested frequency and cell_methods
        self.table_id = self._get_attr("table_id")
        self.frequency = self._get_var_attr(self.varname, "frequency", False)
        if not self.frequency:
            self.frequency = self._get_attr("frequency")
        # In case of unset table_id -
        #  in some projects (eg. CORDEX), the table_id is not required,
        #  since there is one table per frequency, so table_id = frequency.
        if self.table_id == "unknown":
            possible_ids = [key for key in self.CT.keys() if self.frequency in key]
            if len(possible_ids) == 1:
                if self.debug:
                    print("Determined possible table_id = ", possible_ids[0])
                self.table_id = possible_ids[0]
        self.cell_methods = self._get_var_attr(self.varname, "cell_methods", "unknown")
        # Get missing_value
        if self.table_id == "unknown":
            self.missing_value = None
        else:
            self.missing_value = float(
                self.CT[self.table_id]["Header"]["missing_value"]
            )

    def _initialize_time_info(self):
        """Get information about the infile time axis."""
        try:
            self.time = self.xrds.cf["time"]
        except KeyError:
            self.time = None
        if self.time is not None:
            time_attrs = ChainMap(self.time.attrs, self.time.encoding)
            self.calendar = time_attrs.get("calendar", None)
            self.timeunits = time_attrs.get("units", None)
            self.timebnds = time_attrs.get("bounds", None)
            self.timedec = xr.decode_cf(
                self.xrds.copy(deep=True), decode_times=True, use_cftime=True
            ).cf["time"]
        else:
            self.calendar = None
            self.timeunits = None
            self.timebnds = None
            self.timedec = None

    def _initialize_coords_info(self):
        """Get information about the infile coordinates."""
        # Compile list of coordinates from coords, axes and formula_terms
        #  also check for redundant bounds / coordinates
        self.coords = []
        self.bounds = set()
        self.coords_redundant = dict()
        self.bounds_redundant = dict()
        for bkey, bval in self.xrds.cf.bounds.items():
            if len(bval) > 1:
                self.bounds_redundant[bkey] = bval
            self.bounds.update(bval)
        # ds.cf.coordinates
        # {'longitude': ['lon'], 'latitude': ['lat'], 'vertical': ['height'], 'time': ['time']}
        for ckey, clist in self.xrds.cf.coordinates.items():
            _clist = [c for c in clist if c not in self.bounds]
            if len(_clist) > 1:
                self.coords_redundant[ckey] = _clist
            if _clist[0] not in self.coords:
                self.coords.append(_clist[0])
        # ds.cf.axes
        # {'X': ['rlon'], 'Y': ['rlat'], 'Z': ['height'], 'T': ['time']}
        for ckey, clist in self.xrds.cf.axes.items():
            if len(clist) > 1:
                if ckey not in self.coords_redundant:
                    self.coords_redundant[ckey] = clist
            if clist[0] not in self.coords:
                self.coords.append(clist[0])
        # ds.cf.formula_terms
        # {"lev": {"a":"ab", "ps": "ps",...}}
        for akey in self.xrds.cf.formula_terms.keys():
            for ckey, cval in self.xrds.cf.formula_terms[akey].items():
                if cval not in self.coords:
                    self.coords.append(cval)

        # Get the external variables
        self.external_variables = self._get_attr("external_variables", "").split()

        # Update list of variables
        self.varname = [
            v for v in self.varname if v not in self.coords and v not in self.bounds
        ]

    def _get_attr(self, attr, default="unknown"):
        """Get nc attribute."""
        try:
            return self.dataset.getncattr(attr)
        except AttributeError:
            return default

    def _get_var_attr(self, var, attr, default="unknown"):
        """Get nc variable attribute."""
        if self.table_id != "unknown":
            if len(self.varname) > 0:
                try:
                    return self.CT[self.table_id]["variable_entry"][self.varname[0]][
                        attr
                    ]
                except KeyError:
                    return default
        return default

    def _infer_frequency(self):
        """Infer frequency from given time dimension"""
        try:
            return xr.infer_freq(self.timedec)
        except ValueError:
            return "unknown"

    def _read_CV(self, path, table_prefix, table_name):
        """Reads the specified CV table."""
        table_path = Path(path, f"{table_prefix}_{table_name}.json")
        try:
            with open(table_path) as f:
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
        # CMIP6 only and not considered here:
        # 6 # key (source_id) -> *dict key -> dict key (license_info) -> dict key (id, license) -> value
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
        fps = os.path.dirname(self.filepath).split(os.sep)
        for i in range(-1, -len(drs_path_template) - 1, -1):
            try:
                self.drs_dir[drs_path_template[i]] = fps[i]
            except IndexError:
                self.drs_dir[drs_path_template[i]] = False

        # Map DRS filename elements
        self.drs_fn = {}
        fns = os.path.basename(self.filepath).split(".")[0].split("_")
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
        """DRS building blocks in filename and path checked against CV."""
        desc = "DRS (CV)"
        level = BaseCheck.HIGH
        out_of = 3
        score = 0
        messages = []

        # File suffix
        suffix = ".".join(os.path.basename(self.filepath).split(".")[1:])
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
        unchecked = [
            key
            for key in self.drs_dir.keys()
            if not drs_dir_checked[key] and key not in self.global_attrs_hard_checks
        ]
        if len(unchecked) == 0:
            score += 1
        else:
            messages.append(
                f"""DRS path building blocks could not be checked: {', '.join(f"'{ukey}'" for ukey in unchecked)}."""
            )

        # Unchecked DRS filename building blocks
        unchecked = [
            key
            for key in self.drs_fn.keys()
            if not drs_fn_checked[key] and key not in self.global_attrs_hard_checks
        ]
        if len(unchecked) == 0:
            score += 1
        else:
            messages.append(
                f"""DRS filename building blocks could not be checked: {', '.join(f"'{ukey}'" for ukey in unchecked)}."""
            )

        return self.make_result(level, score, out_of, desc, messages)

    def check_drs_consistency(self, ds):
        """DRS building blocks in filename, path and global attributes checked for consistency."""
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

    def check_variable(self, ds):
        """Checks if all variables in the file are part of the CV."""
        desc = "Present variables"
        level = BaseCheck.HIGH
        out_of = 4
        score = 0
        messages = []

        # Check number of requested variables in file
        if len(self.varname) > 1:
            messages.append(
                "More than one requested variable found in file: "
                f"{', '.join(self.varname)}. Only the first one will be checked."
            )
        elif len(self.varname) == 0:
            messages.append("No requested variable could be identified in the file.")
        else:
            score += 1

        # Redundant coordinates /  bounds
        if len(self.coords_redundant.keys()) > 0:
            for key in self.coords_redundant.keys():
                messages.append(
                    f"Multiple coordinate variables found for '{key}': {', '.join(list(self.coords_redundant[key]))}"
                )
        else:
            score += 1
        if len(self.bounds_redundant.keys()) > 0:
            for key in self.bounds_redundant.keys():
                messages.append(
                    f"Multiple bound variables found for '{key}': {', '.join(list(self.bounds_redundant[key]))}"
                )
        else:
            score += 1

        # Create list of all CV coordinates, grids, formula_terms
        cvars = []
        for entry in self.CTgrids["axis_entry"].keys():
            cvars.append(self.CTgrids["axis_entry"][entry]["out_name"])
        for entry in self.CTgrids["variable_entry"].keys():
            cvars.append(self.CTgrids["variable_entry"][entry]["out_name"])
        for entry in self.CTcoords["axis_entry"].keys():
            cvars.append(self.CTcoords["axis_entry"][entry]["out_name"])
        for entry in self.CTformulas["formula_entry"].keys():
            cvars.append(self.CTformulas["formula_entry"][entry]["out_name"])
        cvars = set(cvars)
        # Add grid_mapping
        if len(self.varname) > 0:
            crs = getattr(ds.variables[self.varname[0]], "grid_mapping", False)
            if crs:
                cvars |= {crs}
        # Identify unknown variables / coordinates
        unknown = []
        for var in ds.variables.keys():
            if var not in cvars and var not in self.varname and var not in self.bounds:
                unknown.append(var)
        if len(unknown) > 0:
            messages.append(
                f"(Coordinate) variable(s) {', '.join(unknown)} is/are not part of the CV."
            )
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

    def check_missing_value(self, ds):
        """Checks missing value."""
        desc = "Missing values"
        level = BaseCheck.HIGH
        out_of = 3
        score = 0
        messages = []

        # Check '_FillValue' and 'missing_value'
        if len(self.varname) > 0:
            fval = ChainMap(
                self.xrds[self.varname[0]].attrs, self.xrds[self.varname[0]].encoding
            ).get("_FillValue", None)
            mval = ChainMap(
                self.xrds[self.varname[0]].attrs, self.xrds[self.varname[0]].encoding
            ).get("missing_value", None)
            if fval is None or mval is None:
                messages.append(
                    f"Both, 'missing_value' and '_FillValue' have to be set for variable '{self.varname[0]}'."
                )
            elif fval != mval:
                score += 1
                messages.append(
                    f"The variable attributes '_FillValue' and 'missing_value' differ for variable "
                    f"'{self.varname[0]}': '{fval}' and '{mval}', respectively."
                )
            else:
                score += 2
            if self.missing_value and (fval or mval):
                if not (
                    np.isclose(self.missing_value, fval)
                    and np.isclose(self.missing_value, mval)
                ):
                    messages.append(
                        f"The variable attributes '_FillValue' and/or 'missing_value' differ from "
                        f"the requested value ('{self.missing_value}'): '{fval}' and/or '{mval}', respectively."
                    )
                else:
                    score += 1
            else:
                score += 1
        else:
            score += 3

        return self.make_result(level, score, out_of, desc, messages)

    def check_time_continuity(self, ds):
        """Checks if there are missing timesteps"""
        desc = "Time continuity (within file)"
        level = BaseCheck.HIGH
        out_of = 1
        score = 0
        messages = []

        # Check if frequency is known and supported
        #  (as defined in deltdic)
        if self.frequency in ["unknown", "fx"]:
            return self.make_result(level, out_of, out_of, desc, messages)
        if self.frequency not in deltdic.keys():
            messages.append(f"Frequency '{self.frequency}' not supported.")
            return self.make_result(level, score, out_of, desc, messages)

        # Get the time dimension, calendar and units
        if self.time is None:
            messages.append("Coordinate variable 'time' not found in file.")
            return self.make_result(level, score, out_of, desc, messages)
        if self.calendar is None:
            messages.append("'time' variable has no 'calendar' attribute.")
        if self.timeunits is None:
            messages.append("'time' variable has no 'units' attribute.")
        if len(messages) > 0:
            return self.make_result(level, score, out_of, desc, messages)

        if self.time.size == 0:
            # Empty time axis
            messages.append(f"Time axis '{self.time.name}' has no entries.")
            return self.make_result(level, score, out_of, desc, messages)
        elif self.time.size == 1:
            # No check necessary
            return self.make_result(level, out_of, out_of, desc, messages)
        else:
            deltfs = cftime.num2date(
                self.time.values[1:], units=self.timeunits, calendar=self.calendar
            ) - cftime.num2date(
                self.time.values[:-1], units=self.timeunits, calendar=self.calendar
            )
            deltfs = get_tseconds_vector(deltfs)
            ta = np.ones(len(deltfs) + 1, np.float64)
            ta[:-1] = deltfs[:]
            ta[-1] = deltdic[self.frequency + "min"]
            tb = xr.DataArray(data=ta, dims=["time"], coords=dict(time=self.time))
            tc = xr.where(tb < deltdic[self.frequency + "min"], 1, 0)
            te = xr.where(tb > deltdic[self.frequency + "max"], 1, 0)
            tf = tc + te
            tg = tb.time.where(tf > 0, drop=True)
            th = tb.where(tf > 0, drop=True)
            for tstep in range(0, th.size):
                messages.append(
                    f"Discontinuity in time axis (frequency: '{self.frequency}')  - "
                    f"{cftime.num2date(tg.values[tstep], calendar=self.calendar, units=self.timeunits)}"
                    f" delta-t {printtimedelta(th.values[tstep])} from next timestep!"
                )

            if len(messages) == 0:
                score += 1
            return self.make_result(level, score, out_of, desc, messages)

    def check_time_bounds(self, ds):
        """Checks time bounds for continuity"""
        desc = "Time bounds continuity (within file)"
        level = BaseCheck.HIGH
        out_of = 3
        score = 0
        messages = []

        # Check if frequency is known and supported
        #  (as defined in deltdic)
        if self.frequency in ["unknown", "fx"]:
            return self.make_result(level, out_of, out_of, desc, messages)
        if self.frequency not in deltdic.keys():
            messages.append(f"Frequency '{self.frequency}' not supported.")
            return self.make_result(level, score, out_of, desc, messages)
        if self.cell_methods == "unknown":
            if len(self.varname) > 0:
                messages.append(
                    "No 'cell_methods' attribute defined for '{self.varname[0]}'."
                )
            else:
                messages.append("The 'cell_methods' are not specified.")
        elif "time: point" in self.cell_methods:
            return self.make_result(level, out_of, out_of, desc, messages)

        # Get the time dimension, calendar and units
        if self.time is None:
            messages.append("Coordinate variable 'time' not found in file.")
            return self.make_result(level, score, out_of, desc, messages)
        if self.calendar is None:
            messages.append("'time' variable has no 'calendar' attribute.")
        if self.timeunits is None:
            messages.append("'time' variable has no 'units' attribute.")
        if len(messages) > 0:
            return self.make_result(level, score, out_of, desc, messages)
        if self.timebnds is None:
            messages.append(
                "No bounds could be identified for the time coordinate variable."
            )
            return self.make_result(level, score, out_of, desc, messages)

        # Check time bounds dimensions
        time_bnds = self.xrds[self.timebnds]
        if self.time.dims[0] != time_bnds.dims[0]:
            messages.append(
                "The time coordinate variable and its bounds have a different first dimension."
            )
        elif self.time.size == 0:
            messages.append(f"Time axis '{self.time.name}' has no entries.")
        if len(time_bnds.dims) != 2 or time_bnds.sizes[time_bnds.dims[1]] != 2:
            messages.append(
                "The time bounds variable needs to be two dimensional with the second dimension being of size 2."
            )
        if len(messages) > 0:
            return self.make_result(level, score, out_of, desc, messages)

        # Check for overlapping bounds
        if self.time.size == 1:
            score += 1
        else:
            deltb = time_bnds[1:, 0].values - time_bnds[:-1, 1].values
            overlap_idx = np.where(deltb != 0)[0]
            if len(overlap_idx) == 0:
                score += 1
            else:
                for oi in overlap_idx:
                    messages.append(
                        f"The time bounds overlap between index '{oi}' ('"
                        f"{cftime.num2date(self.time.values[oi], calendar=self.calendar, units=self.timeunits)}"
                        f"') and index '{oi+1}' ('"
                        f"{cftime.num2date(self.time.values[oi+1], calendar=self.calendar, units=self.timeunits)}')."
                    )

        # Check if time values are centered within their respective bounds
        delt = (
            self.time.values[:]
            + self.time.values[:]
            - time_bnds[:, 1]
            - time_bnds[:, 0]
        )
        if np.all(delt == 0):
            score += 1
        else:
            uncentered_idx = np.where(delt != 0)[0]
            for ui in uncentered_idx:
                messages.append(
                    f"For timestep with index '{ui}' ('"
                    f"{cftime.num2date(self.time.values[ui], calendar=self.calendar, units=self.timeunits)}"
                    "'), the time value is not centered within its respective bounds."
                )

        # Check if time bounds are strong monotonically increasing
        deltb = time_bnds[:, 1].values - time_bnds[:, 0].values
        if np.all(deltb > 0):
            score += 1
        else:
            nonmonotonic_idx = np.where(deltb <= 0)[0]
            for ni in nonmonotonic_idx:
                messages.append(
                    f"The time bounds for timestep with index '{ni}' "
                    f"('{cftime.num2date(self.time.values[ni], calendar=self.calendar, units=self.timeunits)}"
                    "') are not strong monotonically increasing."
                )

        return self.make_result(level, score, out_of, desc, messages)

    def check_time_range(self, ds):
        """
        Checks if the time_range element of the filename matches the time axis defined in the file.
        """
        desc = "Time range consistency"
        level = BaseCheck.HIGH
        out_of = 3
        score = 0
        messages = []

        # This check only ensures, that if the data is not time invariant,
        #   a time_range element is present in the filename and it is of format
        #   <time_stamp_0>-<time_stamp_n>, with these time stamps being
        #   a string representation (of equal length) of the first and last
        #   time values, respectively.
        # Another check against definitions in a DRS document / archive specification
        #   document needs to be implemented, to ensure that the length of the
        #   time_stamp strings matches the frequency.

        # Check if frequency is identified and data is not time invariant
        #  (as defined in deltdic)
        if self.frequency in ["unknown", "fx"]:
            return self.make_result(level, out_of, out_of, desc, messages)

        # Check if time_range could be infered from filename
        if not self.drs_fn["time_range"]:
            messages.append("No 'time_range' element could be inferred from filename.")
            return self.make_result(level, score, out_of, desc, messages)
        else:
            score += 1

        # Check if time_range is of format <time_stamp_0>-<time_stamp_n>
        time_range_arr = self.drs_fn["time_range"].split("-")
        if len(time_range_arr) != 2 or len(time_range_arr[0]) != len(time_range_arr[1]):
            messages.append(
                "The 'time_range' element is not of format <time_stamp_0>-<time_stamp_n>."
            )
            return self.make_result(level, score, out_of, desc, messages)
        else:
            score += 1

        # Get the time axis, calendar and units
        if any([tm is None for tm in [self.time, self.calendar, self.timeunits]]):
            # Check cannot be continued, but this error will be raised in another check
            score += 1
            return self.make_result(level, score, out_of, desc, messages)

        # Check if the time_range element matches with the time values
        format = "%4Y%2m%2d%2H%2M"
        t0 = cftime.num2date(
            self.time.values[0], calendar=self.calendar, units=self.timeunits
        )
        t1 = cftime.num2date(
            self.time.values[-1], calendar=self.calendar, units=self.timeunits
        )
        time_range_str = (
            f"{t0.strftime(format=format)[:len(time_range_arr[0])]}-"
            f"{t1.strftime(format=format)[:len(time_range_arr[0])]}"
        )
        if self.drs_fn["time_range"] == time_range_str:
            score += 1
        else:
            messages.append(
                f"The 'time_range' element in the filename ('{self.drs_fn['time_range']}') "
                f"does not match with the first and last time values: '{t0}' and '{t1}'."
            )
        return self.make_result(level, score, out_of, desc, messages)
