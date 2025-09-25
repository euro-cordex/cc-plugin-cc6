import json
import os
import re
from collections import ChainMap
from datetime import datetime as dt
from hashlib import md5
from pathlib import Path

import cf_xarray  # noqa
import cftime
import numpy as np
import xarray as xr
from compliance_checker.base import BaseCheck, BaseNCCheck, Result

from cc_plugin_cc6 import __version__

from ._constants import deltdic
from .utils import match_pattern_or_string

get_tseconds = lambda t: t.total_seconds()  # noqa
get_tseconds_vector = np.vectorize(get_tseconds)
get_abs_tseconds = lambda t: abs(t.total_seconds())  # noqa
get_abs_tseconds_vector = np.vectorize(get_abs_tseconds)


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


def flatten(lst):
    result = []
    for item in lst:
        if isinstance(item, list):
            result.extend(flatten(item))
        else:
            result.append(item)
    return result


class MIPCVCheckBase(BaseCheck):
    register_checker = False
    _cc_spec = "mip"
    _cc_spec_version = __version__
    _cc_description = "Checks compliance with given CV tables."
    _cc_checker_version = __version__
    _cc_display_headers = {3: "Required", 2: "Recommended", 1: "Suggested"}


class MIPCVCheck(BaseNCCheck, MIPCVCheckBase):
    register_checker = True

    @classmethod
    def make_result(cls, level, score, out_of, name, messages):
        return Result(level, (score, out_of), name, messages)

    def setup(self, dataset):
        # netCDF4.Dataset
        self.dataset = dataset
        # Get path to the dataset
        self.filepath = os.path.realpath(
            os.path.normpath(os.path.expanduser(self.dataset.filepath()))
        )
        # xarray.Dataset
        self.xrds = xr.open_dataset(
            self.filepath, decode_coords=True, decode_times=False
        )
        # Options
        if "debug" in self.options:
            self.debug = True
        else:
            self.debug = False
        # Input options
        # - Output for consistency checks across files
        self.consistency_output = self.options.get("consistency_output", False)
        # - Get path to the tables and initialize
        if self.options.get("tables", False):
            tables_path = self.options["tables"]
            self._initialize_CV_info(tables_path)
            self._initialize_time_info()
            self._initialize_coords_info()
            if self.consistency_output:
                self._write_consistency_output()
        # if only the time checks should be run (so no verification against CV / MIP tables)
        elif self.options.get("time_checks_only", False):
            self.varname = [
                var
                for var in flatten(list(self.xrds.cf.standard_names.values()))
                if var
                not in flatten(
                    list(self.xrds.cf.coordinates.values())
                    + list(self.xrds.cf.axes.values())
                    + list(self.xrds.cf.bounds.values())
                    + list(self.xrds.cf.formula_terms.values())
                )
            ]
            self._initialize_time_info()
            self._initialize_coords_info()
            self.frequency = self._get_attr("frequency")
            if self.varname != []:
                self.cell_methods = self.xrds[self.varname[0]].attrs.get(
                    "cell_methods", "unknown"
                )
            else:
                self.cell_methods = "unknown"
            self.drs_fn = {}
            if self.frequency == "unknown" and self.time is not None:
                if self.time.sizes[self.time.dims[0]] > 1 and 1 == 2:
                    for ifreq in [
                        fkey
                        for fkey in deltdic.keys()
                        if "max" not in fkey and "min" not in fkey
                    ]:
                        try:
                            intv = abs(
                                get_tseconds(
                                    cftime.num2date(
                                        self.time.values[1],
                                        units=self.timeunits,
                                        calendar=self.calendar,
                                    )
                                    - cftime.num2date(
                                        self.time.values[0],
                                        units=self.timeunits,
                                        calendar=self.calendar,
                                    )
                                )
                            )
                            if (
                                intv <= deltdic[ifreq + "max"]
                                and intv >= deltdic[ifreq + "min"]
                            ):
                                self.frequency = ifreq
                                break
                        except (AttributeError, ValueError):
                            continue
                elif self.timebnds and len(self.xrds[self.timebnds].dims) == 2:
                    for ifreq in [
                        fkey
                        for fkey in deltdic.keys()
                        if "max" not in fkey and "min" not in fkey
                    ]:
                        try:
                            intv = abs(
                                get_tseconds(
                                    cftime.num2date(
                                        self.xrds[self.timebnds].values[0, 1],
                                        units=self.timeunits,
                                        calendar=self.calendar,
                                    )
                                    - cftime.num2date(
                                        self.xrds[self.timebnds].values[0, 0],
                                        units=self.timeunits,
                                        calendar=self.calendar,
                                    )
                                )
                            )
                            if (
                                intv <= deltdic[ifreq + "max"]
                                and intv >= deltdic[ifreq + "min"]
                            ):
                                self.frequency = ifreq
                                break
                        except (AttributeError, ValueError):
                            continue
            if self.consistency_output:
                self._write_consistency_output()
        # in case of general "mip" checks, the path to the CMOR tables need to be specified
        elif self._cc_spec == "mip":
            raise Exception(
                "ERROR: No 'tables' option specified. Cannot initialize CV and MIP tables."
            )

        # Specify the global attributes that will be checked by a specific check
        #  rather than a general check against the value given in the CV
        #  (i.e. because it is not explicitly defined in the CV)
        self.global_attrs_hard_checks = [
            "creation_date",
            "time_range",
            "variable_id",
            "version",
        ]

        # General
        self.dtypesdict = {
            "integer": np.int32,
            "long": np.int64,
            "real": np.float32,
            "double": np.float64,
            "character": "S",
        }
        self._dtypesdict = {
            **self.dtypesdict,
            "character": str,
        }

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
                    raise KeyError(
                        f"CMOR table '{table}' misses the key '{key}' in the header information."
                    )
        # Compile varlist for quick reference
        varlist = list()
        for table in table_names:
            if table in ["CV", "grids", "coordinate", "formula_terms"]:
                continue
            varlist = varlist + [
                v["out_name"] for v in self.CT[table]["variable_entry"].values()
            ]
        varlist = set(varlist)
        # Map DRS building blocks to the filename, filepath and global attributes
        self._map_drs_blocks()
        # Identify variable name(s)
        var_ids = [v for v in varlist if v in list(self.dataset.variables.keys())]
        self.varname = var_ids
        # Identify table_id, requested frequency and cell_methods
        self.table_id_raw = self._get_attr("table_id")
        if self.table_id_raw in self.CT:
            self.table_id = self.table_id_raw
        else:
            self.table_id = "unknown"
        self.frequency = self._get_var_attr("frequency", False)
        if not self.frequency:
            self.frequency = self._get_attr("frequency")
        # In case of unset table_id -
        #  in some projects (eg. CORDEX), the table_id is not required,
        #  since there is one table per frequency, so table_id = frequency.
        if self.table_id == "unknown":
            possible_ids = list()
            if len(self.varname) > 0:
                for table in table_names:
                    if table in ["CV", "grids", "coordinate", "formula_terms"]:
                        continue
                    if (
                        self.varname[0] in self.CT[table]["variable_entry"]
                        and self.frequency
                        == self.CT[table]["variable_entry"][self.varname[0]][
                            "frequency"
                        ]
                    ):
                        possible_ids.append(table)
            if len(possible_ids) == 0:
                possible_ids = [key for key in self.CT.keys() if self.frequency in key]
            if len(possible_ids) == 1:
                if self.debug:
                    print("Determined possible table_id = ", possible_ids[0])
                self.table_id = possible_ids[0]

        self.cell_methods = self._get_var_attr("cell_methods", "unknown")
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
            # Here, xarray decodes the time axis.
            # The entire checker crashes in case of invalid time units
            # todo: catch a possible exception in base._initialize_time_info
            #       and report the problem in any check method
            self.time_invariant_vars = [
                var
                for var in list(self.xrds.data_vars.keys())
                + list(self.xrds.coords.keys())
                if self.time.name not in self.xrds[var].dims and var not in self.varname
            ]
        else:
            self.calendar = None
            self.timeunits = None
            self.timebnds = None
            self.time_invariant_vars = [
                var
                for var in list(self.xrds.data_vars.keys())
                + list(self.xrds.coords.keys())
                if var not in self.varname
            ]

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

    def _get_var_attr(self, attr, default="unknown"):
        """Get CMOR table variable entry attribute."""
        if self.table_id != "unknown":
            if len(self.varname) > 0:
                try:
                    return self.CT[self.table_id]["variable_entry"][self.varname[0]][
                        attr
                    ]
                except KeyError:
                    return default
        return default

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

    def _write_consistency_output(self):
        """Write output for consistency checks across files."""
        # Dictionaries of global attributes and their data types
        if self.options.get("time_checks_only", False):
            required_attributes = {}
        else:
            required_attributes = self.CV.get("required_global_attributes", {})
        file_attrs_req = {
            k: str(v) for k, v in self.xrds.attrs.items() if k in required_attributes
        }
        file_attrs_nreq = {
            k: str(v)
            for k, v in self.xrds.attrs.items()
            if k not in required_attributes
            if k not in ["history"]
        }
        file_attrs_dtypes = {
            k: type(v).__qualname__ for k, v in self.xrds.attrs.items()
        }
        for k in required_attributes:
            if k not in file_attrs_req:
                file_attrs_req[k] = "unset"
            if k not in file_attrs_dtypes:
                file_attrs_dtypes[k] = "unset"
        # Dictionaries of variable attributes and their data types
        var_attrs = {}
        var_attrs_dtypes = {}
        for var in list(self.xrds.data_vars.keys()) + list(self.xrds.coords.keys()):
            var_attrs[var] = {
                key: str(value)
                for key, value in self.xrds[var].attrs.items()
                if key not in ["history"]
            }
            var_attrs_dtypes[var] = {
                key: type(value).__qualname__
                for key, value in self.xrds[var].attrs.items()
                if key not in ["history"]
            }
        # Dictionary of time information
        time_info = {}
        if self.time is not None:
            # Selecting first and last time_bnds value
            #  (ignoring possible flaws in its definition)
            bound0 = None
            boundn = None
            if self.timebnds is not None:
                try:
                    bound0 = self.xrds[self.timebnds].values[0, 0]
                    boundn = self.xrds[self.timebnds].values[-1, -1]
                except IndexError:
                    pass
            time_info = {
                "frequency": self.frequency,
                "units": self.timeunits,
                "calendar": self.calendar,
                "bound0": bound0,
                "boundn": boundn,
                "time0": self.time.values[0],
                "timen": self.time.values[-1],
            }
        # Dictionary of time_invariant variable checksums
        coord_checksums = {}
        for coord_var in self.time_invariant_vars:
            coord_checksums[coord_var] = md5(
                str(self.xrds[coord_var].values.tobytes()).encode("utf-8")
            ).hexdigest()
        # Dictionary of dimension sizes
        dims = dict(self.xrds.sizes)
        # Do not compare time dimension size, only name
        if self.time is not None:
            dimt = self.time.dims[0]
            dims[dimt] = "n"
        # Dictionary of variable data types
        var_dtypes = {}
        for var in list(self.xrds.data_vars.keys()) + list(self.xrds.coords.keys()):
            var_dtypes[var] = str(self.xrds[var].dtype)
        # Write combined dictionary
        with open(self.consistency_output, "w") as f:
            json.dump(
                {
                    "global_attributes": file_attrs_req,
                    "global_attributes_non_required": file_attrs_nreq,
                    "global_attributes_dtypes": file_attrs_dtypes,
                    "variable_attributes": var_attrs,
                    "variable_attributes_dtypes": var_attrs_dtypes,
                    "variable_dtypes": var_dtypes,
                    "dimensions": dims,
                    "coordinates": coord_checksums,
                    "time_info": time_info,
                },
                f,
                indent=4,
            )

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
        if self.debug:
            print(el, val)
        if isinstance(el, str):
            if self.debug:
                print(val, "->0")
            return (match_pattern_or_string(el, str(val)), [], [el])
        # 1 and 2 #
        elif isinstance(el, list):
            if self.debug:
                print(val, "->1 and 2")
            return (any([match_pattern_or_string(eli, str(val)) for eli in el]), [], el)
        # 3 to 6 #
        elif isinstance(el, dict):
            if self.debug:
                print(val, "->3 to 6")
            if val in el.keys():
                # 3 #
                if isinstance(el[val], str):
                    if self.debug:
                        print(val, "->3")
                    return True, [], []
                # 4 to 6 #
                elif isinstance(el[val], dict):
                    if self.debug:
                        print(val, "->4 to 6")
                    return True, list(el[val].keys()), []
                else:
                    raise ValueError(
                        f"Unknown CV structure for element: {el} and value {val}."
                    )
            else:
                return False, [], list(el.keys())
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
                test, attrs_lvl2, allowed_vals = self._compare_CV_element(
                    self.CV[attr], dic2comp[attr]
                )
                # If comparison fails
                if not test:
                    if len(allowed_vals) == 1:
                        errmsg += f""" Expected value/pattern: '{allowed_vals[0]}'."""
                    elif len(allowed_vals) > 3:
                        errmsg += f""" Allowed values: {", ".join(f"'{av}'" for av in allowed_vals[0:3])}, ..."""
                    elif len(allowed_vals) > 1:
                        errmsg += f""" Allowed values: {", ".join(f"'{av}'" for av in allowed_vals)}."""
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
                                test, attrs_lvl3, allowed_vals = (
                                    self._compare_CV_element(
                                        self.CV[attr][dic2comp[attr]][attr_lvl2],
                                        dic2comp[attr_lvl2],
                                    )
                                )
                            except ValueError:
                                raise ValueError(
                                    f"Unknown CV structure for element {attr} -> {self.CV[attr][dic2comp[attr]][attr_lvl2]} / {attr_lvl2} -> {dic2comp[attr_lvl2]}."
                                )
                            if not test:
                                if len(allowed_vals) == 1:
                                    errmsg_lvl2 += f""" Expected value/pattern: '{allowed_vals[0]}'."""
                                elif len(allowed_vals) > 3:
                                    errmsg_lvl2 += f""" Allowed values: {", ".join(f"'{av}'" for av in allowed_vals[0:3])}, ..."""
                                elif len(allowed_vals) > 1:
                                    errmsg_lvl2 += f""" Allowed values: {", ".join(f"'{av}'" for av in allowed_vals)}."""
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

    def _verify_attrs(self, var, attrsCT, attrs=[]):
        """Compare variable attributes with CMOR table attributes"""
        messages = list()
        varattrs = ChainMap(
            self.xrds[var].attrs,
            self.xrds[var].encoding,
        )
        if not attrs:
            attrs = [
                "standard_name",
                "long_name",
                "units",
                "cell_methods",
                "cell_measures",
                "comment",
                "type",
            ]
        for attr in attrs:
            if attr not in attrsCT:
                continue
            if attr == "comment":
                if attrsCT["comment"] not in varattrs.get("comment", ""):
                    messages.append(
                        f"The variable attribute '{var}:comment' needs to include the specified comment from the CMOR table."
                    )
            elif attr == "type":
                reqdtype = self.dtypesdict.get("type", False)
                if attrsCT["type"] == "character" and reqdtype:
                    if not self.xrds[var].dtype.kind == reqdtype:
                        messages.append(
                            f"The variable '{var}' has to be of type '{attrsCT['type']}'."
                        )
                elif reqdtype:
                    if not self.xrds[var].dtype == reqdtype:
                        messages.append(
                            f"The variable '{var}' has to be of type '{attrsCT['type']}' ({str(reqdtype)})."
                        )
                    else:
                        raise ValueError(
                            f"Unknown requested data type '{attrsCT['type']}' in CMOR table for variable '{var}'."
                        )
            else:
                if attrsCT[attr] != varattrs.get(attr, ""):
                    messages.append(
                        f"The variable attribute '{var}:{attr}' = '{varattrs.get(attr, 'unset')}' is not equivalent to the value specified in the CMOR table ('{attrsCT[attr]}')."
                    )
        return messages

    def _check_table_id(self, ds):
        """Table ID (CV)"""
        ###
        ### This check is left to the check of required global attributes.
        ### See CMIP6 CMOR Tables as example (has table_id as required and lists valid values)
        ###
        desc = "Table ID"
        level = BaseCheck.HIGH
        out_of = 1
        score = 0
        messages = []

        # Check if table_id is defined, and if it is valid
        #   (and if not, if it could be inferred)
        if self.table_id == "unknown":
            if self.table_id_raw == "unknown":
                messages.append("The global attribute 'table_id' is not defined.")
            else:
                messages.append(
                    f"The CMOR table denoted by the global attribute 'table_id' could not be found: '{self.table_id_raw}'."
                )
        elif self.table_id != self.table_id_raw:
            messages.append(
                "The CMOR table denoted by the global attribute 'table_id' "
                f"is not the expected one ('{self.table_id}'): '{self.table_id_raw}'."
            )
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_drs_CV(self, ds):
        """DRS building blocks in filename and path checked against CV."""
        desc = "DRS (CV)"
        level = BaseCheck.HIGH
        out_of = 5
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
                f"""DRS path building blocks could not be checked: {', '.join(f"'{ukey}'" for ukey in sorted(unchecked))}."""
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
                f"""DRS filename building blocks could not be checked: {', '.join(f"'{ukey}'" for ukey in sorted(unchecked))}."""
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
        out_of = 5
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

        # Check ambiguity of variable_entry in CMOR table
        if self.table_id != "unknown" and self.table_id and len(self.varname) > 0:
            CTvars = [
                v["out_name"]
                for v in self.CT[self.table_id]["variable_entry"].values()
                if v["out_name"] == self.varname[0]
            ]
            if len(CTvars) > 1:
                messages.append(
                    f"More than one variable with outname '{self.varname[0]}' found in CMOR table with id '{self.table_id}': {', '.join(CTvars)}. The checks only consider '{self.varname[0]}', which may lead to incorrect check results."
                )
            elif len(CTvars) == 0:
                messages.append(
                    f"No variable with outname '{self.varname[0]}' found in CMOR table with id '{self.table_id}'."
                )
            else:
                score += 1
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
                f"(Coordinate) variable(s) {', '.join(unknown)} is/are not part of the CV or not compliant with the CF-Conventions."
            )
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_grid_definition(self, ds):
        """Checks definition of variables in the grids table."""
        desc = "Grid definition (CV)"
        level = BaseCheck.HIGH
        score = 0
        out_of = 1
        messages = []

        # Only check if requested variable is identified
        if len(self.varname) == 0:
            return self.make_result(level, out_of, out_of, desc, messages)

        # Check only the first latitude and longitude found
        dimsCT = self._get_var_attr("dimensions", [])
        if "latitude" or "longitude" in dimsCT:
            if (
                "latitude" in self.xrds.cf.standard_names
                and self.xrds[self.xrds.cf.standard_names["latitude"][0]].ndim > 1
            ):
                lat = self.xrds.cf.standard_names["latitude"][0]
                if lat != self.CTgrids["variable_entry"]["latitude"]["out_name"]:
                    messages.append(
                        f"Latitude variable '{lat}' should be named '{self.CTgrids['variable_entry']['latitude']['out_name']}'."
                    )
                messages.extend(
                    self._verify_attrs(lat, self.CTgrids["variable_entry"]["latitude"])
                )
                if lat in self.xrds.cf.bounds:
                    lat_bnds = self.xrds.cf.bounds[lat][0]
                    if (
                        lat_bnds
                        != self.CTgrids["variable_entry"]["vertices_latitude"][
                            "out_name"
                        ]
                    ):
                        messages.append(
                            f"Latitude bounds variable '{lat_bnds}' should be named '{self.CTgrids['variable_entry']['vertices_latitude']['out_name']}'."
                        )
                    messages.extend(
                        self._verify_attrs(
                            lat_bnds,
                            self.CTgrids["variable_entry"]["vertices_latitude"],
                            attrs=["type"],
                        )
                    )
            if (
                "longitude" in self.xrds.cf.standard_names
                and self.xrds[self.xrds.cf.standard_names["longitude"][0]].ndim > 1
            ):
                lon = self.xrds.cf.standard_names["longitude"][0]
                if lon != self.CTgrids["variable_entry"]["longitude"]["out_name"]:
                    messages.append(
                        f"Longitude variable '{lon}' should be named '{self.CTgrids['variable_entry']['longitude']['out_name']}'."
                    )
                messages.extend(
                    self._verify_attrs(lon, self.CTgrids["variable_entry"]["longitude"])
                )
                if lon in self.xrds.cf.bounds:
                    lon_bnds = self.xrds.cf.bounds[lon][0]
                    if (
                        lon_bnds
                        != self.CTgrids["variable_entry"]["vertices_longitude"][
                            "out_name"
                        ]
                    ):
                        messages.append(
                            f"Longitude bounds variable '{lon_bnds}' should be named '{self.CTgrids['variable_entry']['vertices_longitude']['out_name']}'."
                        )
                    messages.extend(
                        self._verify_attrs(
                            lon_bnds,
                            self.CTgrids["variable_entry"]["vertices_longitude"],
                            attrs=["type"],
                        )
                    )
            if "grid_latitude" in self.xrds.cf.standard_names:
                lat = self.xrds.cf.standard_names["grid_latitude"][0]
                if lat != self.CTgrids["axis_entry"]["grid_latitude"]["out_name"]:
                    messages.append(
                        f"Grid latitude variable '{lat}' should be named '{self.CTgrids['axis_entry']['latitude']['out_name']}'."
                    )
                messages.extend(
                    self._verify_attrs(lat, self.CTgrids["axis_entry"]["grid_latitude"])
                )
            if "grid_longitude" in self.xrds.cf.standard_names:
                lon = self.xrds.cf.standard_names["grid_longitude"][0]
                if lon != self.CTgrids["axis_entry"]["grid_longitude"]["out_name"]:
                    messages.append(
                        f"Grid longitude variable '{lon}' should be named '{self.CTgrids['axis_entry']['longitude']['out_name']}'."
                    )
                messages.extend(
                    self._verify_attrs(
                        lon, self.CTgrids["axis_entry"]["grid_longitude"]
                    )
                )
            if "projection_y_coordinate" in self.xrds.cf.standard_names:
                y = self.xrds.cf.standard_names["projection_y_coordinate"][0]
                if y != self.CTgrids["axis_entry"]["y_deg"]["out_name"]:
                    messages.append(
                        f"Projection y coordinate variable '{y}' should be named '{self.CTgrids['axis_entry']['y_deg']['out_name']}'."
                    )
                messages.extend(
                    self._verify_attrs(y, self.CTgrids["axis_entry"]["y_deg"])
                )
            if "projection_x_coordinate" in self.xrds.cf.standard_names:
                x = self.xrds.cf.standard_names["projection_x_coordinate"][0]
                if x != self.CTgrids["axis_entry"]["x_deg"]["out_name"]:
                    messages.append(
                        f"Projection x coordinate variable '{x}' should be named '{self.CTgrids['axis_entry']['x_deg']['out_name']}'."
                    )
                messages.extend(
                    self._verify_attrs(x, self.CTgrids["axis_entry"]["x_deg"])
                )

        if len(messages) == 0:
            score += 1

        return self.make_result(level, out_of, score, desc, messages)

    def _resolve_generic_level(self, dimCT, var, messages):
        """
        Attempt to resolve a generic level like 'alevel' to a valid axis_entry.
        """
        candidates = [
            key
            for key, entry in self.CTcoords["axis_entry"].items()
            if entry.get("generic_level_name") == dimCT
        ]

        if not candidates:
            messages.append(
                f"The required dimension / coordinate '{dimCT}' of variable '{var}' is not defined explicitly and no generic level match (e.g., 'generic_level_name': '{dimCT}') could be found in the CMOR table."
            )
            return {}

        # Get candidates with same standard_name as data set variables to get possible matches
        pmatches = list()
        for c in candidates:
            if (
                self.CTcoords["axis_entry"][c].get("standard_name")
                in self.xrds.cf.standard_names
            ):
                pmatches.append(c)

        if not pmatches:
            messages.append(
                f"The required dimension / coordinate '{dimCT}' of variable '{var}' is not defined explicitly. No generic level matches ({', '.join(candidates)}) could be identified in the input file via standard_name."
            )
            return {}
        elif len(pmatches) > 1:
            # Try to select further by long_name and formula:
            plfmatches = list()
            for pmatch in pmatches:
                if self.CTcoords["axis_entry"][pmatch].get("long_name") == self.xrds[
                    self.xrds.cf.standard_names[
                        self.CTcoords["axis_entry"][pmatch].get("standard_name")
                    ][0]
                ].attrs.get("long_name") and self.CTcoords["axis_entry"][pmatch].get(
                    "formula"
                ) == self.xrds[
                    self.xrds.cf.standard_names[
                        self.CTcoords["axis_entry"][pmatch].get("standard_name")
                    ][0]
                ].attrs.get(
                    "formula"
                ):
                    plfmatches.append(pmatch)
            if len(plfmatches) != 1:
                messages.append(
                    f"The required dimension / coordinate '{dimCT}' of variable '{var}' is not defined explicitly. Multiple generic level matches "
                    f"({', '.join(pmatches)}) can be identified due to insufficient and incompliant metadata specification."
                )
                return {}
            else:
                return self.CTcoords["axis_entry"][plfmatches[0]]

        return self.CTcoords["axis_entry"][pmatches[0]]

    def check_variable_definition(self, ds):
        """Checks mandatory variable attributes of the main variable and associated coordinates."""
        desc = "Variable and coordinate definition (CV)"
        level = BaseCheck.HIGH
        score = 0
        out_of = 1
        messages = []

        # Only check if requested variable is identified
        if len(self.varname) == 0:
            return self.make_result(level, out_of, out_of, desc, messages)

        var = self.varname[0]
        attrs = ChainMap(
            self.xrds[self.varname[0]].attrs,
            self.xrds[self.varname[0]].encoding,
        )
        dims = list(self.xrds[self.varname[0]].dims)
        coords_raw = attrs.get("coordinates", "")
        coords = [
            str(cv)
            for cv in (
                coords_raw
                if isinstance(coords_raw, list)
                else coords_raw.split() if isinstance(coords_raw, str) else []
            )
        ]

        # Check dimensions & coordinates attribute
        # todo: check coordinate attributes
        # todo: support generic levels like "alevel" / "olevel" / "alevhalf" incl formula terms
        # todo: check max min range for var / coord
        #
        dimsCT = self._get_var_attr("dimensions", [])
        dimsCT_is_valid = True
        if isinstance(dimsCT, str):
            dimsCT = dimsCT.split()
        elif not isinstance(dimsCT, list):
            messages.append(
                f"Invalid 'dimensions' format for variable '{var}'. This is an issue in the CMOR tables definition and not necessarily in the data file."
            )
            dimsCT_is_valid = False
        if dimsCT and dimsCT_is_valid:
            for dimCT in dimsCT:
                # The coordinate out_name must be in one of the following
                # - in the variable dimensions
                # - in the variable attribute "coordinates"
                diminfo = self.CTcoords["axis_entry"].get(dimCT, {})
                if not diminfo:
                    diminfo = self._resolve_generic_level(dimCT, var, messages)
                    # todo: checks below need to be updated to support generic levels
                    continue
                # if not diminfo:  # if checks below support generic levels, this can be uncommented
                #    continue
                dim_on = diminfo.get("out_name", "")
                dim_val_raw = diminfo.get("value", "")
                dim_bnds_raw = diminfo.get("bounds_values", "")
                dim_type = diminfo.get("type", "")
                dim_req = diminfo.get("requested", "")
                dim_reqbnds = diminfo.get("requested_bounds", "")
                dim_mhbnds = diminfo.get("must_have_bounds", "")
                cbnds = self.xrds[dim_on].attrs.get("bounds", None)
                if dim_mhbnds not in ["yes", "no"]:
                    messages.append(
                        f"The 'must_have_bounds' attribute of dimension / coordinate '{dimCT}' of the variable '{var}' has to be set to 'yes' or 'no'. "
                        "This is an issue in the CMOR tables definition and not necessarily in the data file."
                    )
                    continue
                if not dim_on:
                    messages.append(
                        f"The 'out_name' of dimension / coordinate '{dimCT}' of the variable '{var}' cannot be inferred from the CMOR table."
                    )
                    continue
                if dim_on not in self.xrds:
                    messages.append(
                        f"The coordinate variable '{dim_on}' for dimension / coordinate '{dimCT}' of the variable '{var}' cannot be found in the data file."
                    )
                    continue
                # Get required coordinate values from CMOR table
                dim_val = [
                    str(dv)
                    for dv in (
                        dim_val_raw
                        if isinstance(dim_val_raw, list)
                        else dim_val_raw.split() if isinstance(dim_val_raw, str) else []
                    )
                ]
                dim_bnds = [
                    str(dv)
                    for dv in (
                        dim_bnds_raw
                        if isinstance(dim_bnds_raw, list)
                        else (
                            dim_bnds_raw.split()
                            if isinstance(dim_bnds_raw, str)
                            else []
                        )
                    )
                ]
                # Test definition and value for singleton dimensions / scalar coordinates
                if dim_val and dim_req:
                    messages.append(
                        f"The 'value' and 'requested' attributes of dimension / coordinate '{dimCT}' of the variable '{var}' cannot be set at the same time. This is an issue in the CMOR tables definition and not necessarily in the data file."
                    )
                    continue
                if (dim_val or dim_req) and not dim_type:
                    messages.append(
                        f"The 'type' of dimension / coordinate '{dimCT}' of the variable '{var}' cannot be inferred from the CMOR table. This is an issue in the CMOR tables definition and not necessarily in the data file."
                    )
                    continue
                if dim_bnds and not dim_val:
                    messages.append(
                        f"The 'bounds_values' of dimension / coordinate '{dimCT}' of the variable '{var}' is defined while 'value' is not. This is an issue in the CMOR tables definition and not necessarily in the data file."
                    )
                    continue
                if dim_bnds and dim_mhbnds != "yes":
                    messages.append(
                        f"The 'bounds_values' of dimension / coordinate '{dimCT}' of the variable '{var}' is defined while 'must_have_bounds' is not set to 'True'. "
                        "This is an issue in the CMOR tables definition and not necessarily in the data file."
                    )
                    continue
                if dim_mhbnds == "yes":
                    if not cbnds:
                        messages.append(
                            f"The dimension / coordinate '{dimCT}' of the variable '{var}' requires bounds to be defined."
                        )
                    elif cbnds not in self.xrds:
                        messages.append(
                            f"The bounds variable '{cbnds}' of dimension / coordinate '{dimCT}' of the variable '{var}'. is missing in the data file."
                        )
                    if (
                        cbnds
                        and (
                            (self.xrds[dim_on].ndim == 0 and self.xrds[cbnds].ndim == 1)
                            or (
                                self.xrds[dim_on].ndim == 1
                                and self.xrds[cbnds].ndim == 2
                                and all(
                                    [
                                        self.xrds[cbnds].sizes[idim] == 2
                                        for idim in self.xrds[cbnds].dims
                                        if idim not in self.xrds[dim_on].dims
                                    ]
                                )
                            )
                        )
                        and cbnds != dim_on + "_bnds"
                    ):
                        messages.append(
                            f"The bounds variable '{cbnds}' of dimension / coordinate '{dimCT}' of the variable '{var}' should be named '{dim_on}_bnds'."
                        )
                elif dimCT == "time":
                    if cbnds:
                        messages.append(
                            f"The dimension / coordinate '{dimCT}' of the variable '{var}' should not have bounds defined ('{cbnds}')."
                        )
                if dim_val and len(dim_val) == 1:
                    if dim_on in dims:
                        messages.append(
                            f"The dimension '{dim_on}' of the variable '{var}' is a singleton dimension but should be defined as a scalar coordinate instead."
                        )
                    if dim_on not in coords:
                        if dim_on in dims:
                            messages.append(
                                f"The coordinate variable '{dim_on}' of the variable '{var}' is a scalar coordinate and should be listed under the '{var}:coordinates' variable attribute."
                            )
                    if dim_on in self.xrds:
                        if self.xrds[dim_on].values.ndim != 0:
                            messages.append(
                                f"The coordinate variable '{dim_on}' of the variable '{var}' should be a scalar coordinate but is not 0-dimensional."
                            )
                        if (
                            self._dtypesdict.get(dim_type, str)(dim_val[0])
                            != np.atleast_1d(self.xrds[dim_on].values)[0]
                        ):
                            messages.append(
                                f"The coordinate variable '{dim_on}' of the variable '{var}' needs to have the value '{self._dtypesdict.get(dim_type, str)(dim_val[0])}', but has the value '{np.atleast_1d(self.xrds[dim_on].values[0])}'."
                            )
                        # check bounds / bounds_values
                        if dim_bnds and cbnds:
                            if len(dim_bnds) != 2:
                                messages.append(
                                    f"The coordinate variable '{dim_on}' of the variable '{var}' has a maldefined 'bounds_values' attribute. Exactly two bounds values have to be specified. This is an issue in the CMOR tables definition and not necessarily in the data file."
                                )
                            elif (
                                self.xrds[cbnds].ndim != 1
                                or self.xrds.sizes[self.xrds[cbnds].dim[0]] != 2
                            ):
                                messages.append(
                                    f"The bounds variable '{cbnds}' needs to be one-dimensional and have exactly two values."
                                )
                            elif (
                                self._dtypesdict.get(dim_type, str)(dim_bnds[0])
                                != np.atleast_1d(self.xrds[cbnds].values)[0]
                                or self._dtypesdict.get(dim_type, str)(dim_bnds[1])
                                != np.atleast_1d(self.xrds[cbnds].values)[1]
                            ):
                                messages.append(
                                    f"The coordinate variable '{dim_on}' of the variable '{var}' needs to have the value '{dim_val[0]}' for the bounds, but has the value '{dim_bnds[0]}'."
                                )
                    else:
                        messages.append(
                            f"The coordinate variable '{dim_on}' is missing in the data file."
                        )
                elif dim_val and len(dim_val) > 1:
                    messages.append(
                        f"The 'value' attribute in CMOR tables may only be used to define scalar coordinates. However, in this case, the 'value' attribute of dimension / coordinate '{dimCT}' of the variable '{var}' contains more than one value: '{dim_val}'. This is an issue in the CMOR tables definition and not necessarily in the data file."
                    )
                    continue
                # todo: the following cases regarding requested and requested_bounds do not occur for CORDEX and need still some work and proper test data
                elif dim_req and len(dim_req) == 1:
                    messages.append(
                        f"The 'requested' attribute in the CMOR tables defines a singleton dimension for coordinate variable '{dim_on}' of the variable '{var}', however the 'value' attribute should be used in that case to define a scalar coordinate instead. This is an issue in the CMOR tables definition and not necessarily in the data file."
                    )
                    continue
                elif dim_reqbnds and not dim_req:
                    messages.append(
                        f"When the 'requested_bounds' attribute is defined in the CMOR tables, 'requested' needs to be defined as well (for coordinate variable '{dim_on}' of the variable '{var}'). This is an issue in the CMOR tables definition and not necessarily in the data file."
                    )
                    continue
                elif dim_req:
                    # In CF, dimension names and coordinate variable names are the same for coordinate variables - this should hence also be covered by CC CF checks
                    if dim_on not in dims:
                        messages.append(
                            f"The dimension '{dim_on}' of the variable '{var}' should be defined as dimension."
                        )
                    if dim_on in coords:
                        messages.append(
                            f"The dimension '{dim_on}' of the variable '{var}' should not be listed under the '{var}:coordinates' variable attribute."
                        )
                    if dim_on in self.xrds:
                        if dim_val != list(self.xrds[dim_on].values):
                            messages.append(
                                f"The coordinate variable '{dim_on}' of the variable '{var}' needs to have the values '{dim_val}', but has the values '{list(self.xrds[dim_on].values)}'."
                            )
                    else:
                        messages.append(
                            f"The coordinate variable '{dim_on}' is missing in the data file."
                        )
                    # todo: check requested_bounds

        # Check attributes
        for vattr in [
            "standard_name",
            "long_name",
            "units",
            "cell_methods",
            "cell_measures",
            "comment",
            "type",
        ]:
            vattrCT = self._get_var_attr(vattr, False)
            if vattrCT:
                if vattr == "comment":
                    if vattrCT not in attrs.get("comment", ""):
                        messages.append(
                            f"The variable attribute '{var}:comment' needs to include the specified comment from the CMOR table."
                        )
                elif vattr == "type":
                    reqdtype = self.dtypesdict.get(vattrCT, False)
                    if vattrCT == "character" and reqdtype:
                        if not self.xrds[var].dtype.kind == reqdtype:
                            messages.append(
                                f"The variable '{var}' has to be of type '{vattrCT}'."
                            )
                    elif reqdtype:
                        if not self.xrds[var].dtype == reqdtype:
                            messages.append(
                                f"The variable '{var}' has to be of type '{vattrCT}' ({str(reqdtype)})."
                            )
                    else:
                        raise ValueError(
                            f"Unknown requested data type '{vattrCT}' for variable attribute '{var}:{vattr}'."
                        )
                else:
                    if vattrCT != attrs.get(vattr, ""):
                        messages.append(
                            f"The variable attribute '{var}:{vattr} = '{attrs.get(vattr, 'unset')}' is not equivalent to the value specified in the CMOR table ('{vattrCT}')."
                        )
        if len(messages) == 0:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_required_global_attributes(self, ds):
        """Checks presence of mandatory global attributes."""
        desc = "Required global attributes (Presence)"
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

    def check_required_global_attributes_CV(self, ds):
        """Global attributes checked against CV."""
        desc = "Required global attributes (CV)"
        level = BaseCheck.HIGH
        score = 0
        out_of = 2
        messages = []

        required_attributes = self.CV.get("required_global_attributes", {})
        file_attrs = {
            k: v for k, v in self.xrds.attrs.items() if k in required_attributes
        }
        for k in required_attributes:
            if k not in file_attrs:
                file_attrs[k] = "unset"

        # Global attributes
        ga_checked, ga_messages = self._compare_CV(file_attrs, "Global attribute ")
        if len(ga_messages) == 0:
            score += 1
        else:
            messages.extend(ga_messages)

        # Unchecked global attributes
        unchecked = [
            key
            for key in required_attributes
            if not ga_checked[key] and key not in self.global_attrs_hard_checks
        ]
        if len(unchecked) == 0:
            score += 1
        else:
            messages.append(
                f"""Required global attributes could not be checked against CV: {', '.join(f"'{ukey}'" for ukey in sorted(unchecked))}."""
            )

        return self.make_result(level, score, out_of, desc, messages)

    def check_missing_value(self, ds):
        """Checks missing value."""
        desc = "Missing values"
        level = BaseCheck.HIGH
        out_of = 6
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
            # Check that both are set, and if so, are equal
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

            # Check that missing value is equal to requested value and has the correct dtype
            if not mval:
                mval = fval
            elif not fval:
                fval = mval

            if self.missing_value and mval:
                if not (
                    np.isclose(self.missing_value, fval)
                    and np.isclose(self.missing_value, mval)
                ):
                    messages.append(
                        f"The variable attributes '_FillValue' and/or 'missing_value' differ from "
                        f"the requested value '{self.missing_value}'."
                    )
                else:
                    score += 1
            else:
                score += 1

            if fval:
                dtype_fval = fval.dtype
                dtype_mval = mval.dtype
                if dtype_fval != dtype_mval:
                    messages.append(
                        f"The variable attributes '_FillValue' and 'missing_value' have different dtypes: "
                        f"'{dtype_fval}' and '{dtype_mval}', respectively."
                    )
                else:
                    score += 1
                if (
                    dtype_fval != self.xrds[self.varname[0]].dtype
                    or dtype_mval != self.xrds[self.varname[0]].dtype
                ):
                    messages.append(
                        "The variable attributes '_FillValue' and/or 'missing_value' have different data types than the variable."
                    )
                else:
                    score += 1
                if dtype_fval != np.float32 or dtype_mval != np.float32:
                    messages.append(
                        "The variable attributes '_FillValue' and/or 'missing_value' do not have 'float' as data type."
                    )
                else:
                    score += 1
            else:
                score += 3

        else:
            score += 6

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
        if self.frequency == "fx":
            return self.make_result(level, out_of, out_of, desc, messages)
        elif self.frequency == "unknown":
            messages.append("Cannot test time continuity: Frequency not defined.")
            return self.make_result(level, score, out_of, desc, messages)
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
            tindex = xr.DataArray(
                data=range(0, len(self.time)),
                dims=["time"],
                coords=dict(time=self.time),
            )
            tindex_f = tindex.where(tf > 0, drop=True)
            for tstep in range(0, th.size):
                messages.append(
                    f"Discontinuity in time axis (frequency: '{self.frequency}') at index '{tindex_f.isel(time=tstep).item()}'"
                    f" ('{cftime.num2date(tg.isel(time=tstep).item(), calendar=self.calendar, units=self.timeunits)}'):"
                    f" delta-t {printtimedelta(th.values[tstep])} from next timestep!"
                )

            if len(messages) == 0:
                score += 1
            return self.make_result(level, score, out_of, desc, messages)

    def check_time_bounds(self, ds):
        """Checks time bounds for continuity"""
        desc = "Time bounds continuity (within file)"
        level = BaseCheck.HIGH
        out_of = 4
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
            if len(self.varname) > 0 and not self.options.get(
                "time_checks_only", False
            ):
                messages.append(
                    f"MIP table for '{self.varname[0]}' could not be identified"
                    " and thus no 'cell_methods' attribute could be read."
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
        tol = 10e-10
        delt = (
            self.time.values[:]
            + self.time.values[:]
            - time_bnds[:, 1]
            - time_bnds[:, 0]
        )
        if np.all(np.abs(delt) <= tol):
            score += 1
        else:
            uncentered_idx = np.where(np.abs(delt) > tol)[0]
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

        # Check if time interval is as expected
        deltfs = cftime.num2date(
            time_bnds.values[:, 1], units=self.timeunits, calendar=self.calendar
        ) - cftime.num2date(
            time_bnds.values[:, 0], units=self.timeunits, calendar=self.calendar
        )
        deltfs = get_tseconds_vector(deltfs)
        ti = xr.DataArray(data=deltfs, dims=["time"], coords=dict(time=self.time))
        ti_l = xr.where(ti < deltdic[self.frequency + "min"], 1, 0)
        ti_h = xr.where(ti > deltdic[self.frequency + "max"], 1, 0)
        ti_lh = ti_l + ti_h
        ti_t = ti.time.where(ti_lh > 0, drop=True)
        ti_f = ti.where(ti_lh > 0, drop=True)
        tindex = xr.DataArray(
            data=range(0, len(self.time)), dims=["time"], coords=dict(time=self.time)
        )
        tindex_f = tindex.where(ti_lh > 0, drop=True)
        failure = False
        for tstep in range(0, ti_f.size):
            failure = True
            messages.append(
                f"Discontinuity in time bounds (frequency: '{self.frequency}') at index '{int(tindex_f.values[tstep])}"
                f"' ('{cftime.num2date(ti_t.values[tstep], calendar=self.calendar, units=self.timeunits)}'):"
                f" time interval is of size '{printtimedelta(ti_f.values[tstep])}'!"
            )
        if not failure:
            score += 1

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

        # If time_range is not part of the file name structure, abort
        if "time_range" not in self.drs_fn:
            # Attempt to infer time range from filename if only timechecks are to be run:
            if self.options.get("time_checks_only", False):
                matches = list(
                    filter(
                        re.compile(r"^\d{1,}-?\d*$").match,
                        os.path.splitext(os.path.basename(self.filepath))[0].split("_"),
                    )
                )
                if len(matches) != 1:
                    return self.make_result(level, out_of, out_of, desc, messages)
                self.drs_fn = {"time_range": matches[0]}
            else:
                return self.make_result(level, out_of, out_of, desc, messages)

        # Check if frequency is identified and data is not time invariant
        #  (as defined in deltdic)
        if self.frequency in ["unknown", "fx"]:
            if self.frequency == "fx" and self.drs_fn["time_range"]:
                messages.append(
                    "Expected no 'time_range' element in filename for "
                    f"frequency 'fx', but found: '{self.drs_fn['time_range']}'."
                )
                return self.make_result(level, score, out_of, desc, messages)
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

    def check_version_date(self, ds):
        """Checks if the version_date is properly defined."""
        desc = "version_date (CMOR)"
        level = BaseCheck.HIGH
        out_of = 1
        score = 0
        messages = []

        # Check version/version_date in DRS path (format vYYYYMMDD, not in the future)
        if self.drs_dir["version"]:
            if not re.fullmatch(r"^v[0-9]{8}$", self.drs_dir["version"]):
                messages.append(
                    "The 'version' element in the path is not of the format 'vYYYYMMDD':"
                    f" '{self.drs_dir['version']}'."
                )
            elif dt.strptime(self.drs_dir["version"][1:], "%Y%m%d") > dt.now():
                messages.append(
                    f"The 'version' element in the path is in the future:"
                    f" '{self.drs_dir['version'][1:5]}-{self.drs_dir['version'][5:7]}"
                    f"-{self.drs_dir['version'][7:9]}.'"
                )
            else:
                score += 1
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_creation_date(self, ds):
        """Checks if the creation_date is compliant to the archive specifications."""
        desc = "creation_date (CMOR)"
        level = BaseCheck.HIGH
        out_of = 1
        score = 0
        messages = []

        # Check global attribute creation_date (format YYYY-MM-DDTHH:MM:SSZ, not in the future)
        if "creation_date" in self.xrds.attrs:
            if not re.fullmatch(
                r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
                self.xrds.attrs["creation_date"],
            ):
                messages.append(
                    f"The 'creation_date' attribute is not of the format 'YYYY-MM-DDTHH:MM:SSZ': '{self.xrds.attrs['creation_date']}'"
                )
            elif (
                dt.strptime(self.xrds.attrs["creation_date"], "%Y-%m-%dT%H:%M:%SZ")
                > dt.now()
            ):
                messages.append(
                    f"The 'creation_date' attribute is in the future: '{self.xrds.attrs['creation_date']}'"
                )
            else:
                score += 1
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)
