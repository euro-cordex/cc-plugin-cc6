import os
import re
from datetime import timedelta

import cf_xarray  # noqa
import cftime
import numpy as np
import xarray as xr
from compliance_checker.base import BaseCheck

from cc_plugin_cc6 import __version__

from ._constants import deltdic
from .base import MIPCVCheck
from .tables import retrieve

CORDEX_CMIP6_CMOR_TABLES_URL = "https://raw.githubusercontent.com/WCRP-CORDEX/cordex-cmip6-cmor-tables/main/Tables/"


class CORDEXCMIP6(MIPCVCheck):
    register_checker = True
    _cc_spec = "cc6"
    _cc_spec_version = __version__
    _cc_description = "Checks compliance with CORDEX-CMIP6."
    _cc_url = "https://github.com/euro-cordex/cc-plugin-cc6"

    def setup(self, dataset):
        super().setup(dataset)
        if not self.options.get("tables", False) and not self.options.get(
            "time_checks_only", False
        ):
            if self.debug:
                print("Downloading CV and CMOR tables.")
            tables_path = self.options.get(
                "tables_dir", "~/.cc6_metadata/cordex-cmip6-cmor-tables"
            )
            for table in [
                "coordinate",
                "grids",
                "formula_terms",
                "CV",
                "1hr",
                "6hr",
                "day",
                "mon",
                "fx",
            ]:
                filename = "CORDEX-CMIP6_" + table + ".json"
                url = CORDEX_CMIP6_CMOR_TABLES_URL + filename
                filename_retrieved = retrieve(
                    CORDEX_CMIP6_CMOR_TABLES_URL + "CORDEX-CMIP6_" + table + ".json",
                    filename,
                    tables_path,
                    force=self.options.get("force_table_download", False),
                )
                if os.path.basename(os.path.realpath(filename_retrieved)) != filename:
                    raise AssertionError(
                        f"Download failed for CV table '{filename_retrieved}' (source: '{url}')."
                    )

            self._initialize_CV_info(tables_path)
            self._initialize_time_info()
            self._initialize_coords_info()
            if self.consistency_output:
                self._write_consistency_output()

        # Specify the global attributes that will be checked by a specific check
        #  rather than a general check against the value given in the CV
        #  (i.e. because it does not explicitly defined in the CV)
        self.global_attrs_hard_checks = [
            "contact",
            "creation_date",
            "domain_id",
            "grid",
            "institution",
            "time_range",
            "variable_id",
            "version",
            "version_realization",
        ]

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
                f"File format differs from expectation ({data_model_expected}/{disk_format_expected}): "
                f"'{ds.data_model}/{ds.disk_format}'."
            )
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_data_types(self, ds):
        """Checks if the variable and coordinate data types are correct."""
        desc = "Incorrect data type (Archive Specifications)"
        level = BaseCheck.HIGH
        out_of = 2
        score = 0
        messages = []

        # Coordinate variables
        # - ensure that all coordinate variables are of double precision
        # - besides ps/orog as part of formula to describe native vertical axis
        # - grid_mapping is tested in a separate check
        for c in set(self.coords) | set(self.bounds):
            if self.xrds[c].dtype != np.float64:
                if len(self.varname) > 0 and c == getattr(
                    ds.variables[self.varname[0]], "grid_mapping", None
                ):
                    pass
                elif (
                    c
                    in {
                        k: self.xrds.cf.formula_terms.get(k, None)
                        for k in ("ps", "orog")
                    }.values()
                ):
                    pass
                else:
                    messages.append(
                        f"The coordinate variable '{c}' must be of double precision with the data type 'float64' / 'double'."
                    )
            elif (
                c
                in {
                    k: self.xrds.cf.formula_terms.get(k, None) for k in ("ps", "orog")
                }.values()
                and self.xrds[c].dtype != np.float32
            ):
                messages.append(
                    f"The auxiliary coordinate variable '{c}' must be of single precision with the data type 'float32' / 'float'."
                )
        if len(messages) == 0:
            score += 1

        # Main variable needs to be of single precision
        if len(self.varname) > 0:
            if self.xrds[self.varname[0]].dtype != np.float32:
                messages.append(
                    f"The main variable '{self.varname[0]}' must be of single precision with the data type 'float32' / 'float'."
                )
            else:
                score += 1
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
                "It is recommended that data should be compressed with a 'deflate level' of '1' "
                "and enabled 'shuffle' option."
            )
            if ds[varname].filters()["complevel"] < 1:
                messages[-1] += " The data is uncompressed."
            elif ds[varname].filters()["complevel"] > 1:
                messages[-1] += (
                    " The data is compressed with a higher 'deflate level' than recommended, "
                    "this can lead to performance issues when accessing the data."
                )
            if ds[varname].filters()["shuffle"] is False:
                messages[-1] += " The 'shuffle' option is disabled."
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_time_chunking(self, ds):
        """Checks if the chunking with respect to the time dimension is in accordance with Archive Specifications."""
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
            "6hr",
            "day",
            "mon",
        ]:
            messages.append(f"Frequency '{self.frequency}' not supported.")
            return self.make_result(level, score, out_of, desc, messages)

        # Expected last simulation year
        # -> suppress error for last year of a simulation
        #    if it is the "official" last year
        expected_last_sim_year = {
            "evaluation": 2024,
            "historical": 2014,
            "ssp119": 2100,
            "ssp126": 2100,
            "ssp245": 2100,
            "ssp370": 2100,
            "ssp585": 2100,
        }

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

        # Get the first and last time values
        first_time = self.time[0].values
        last_time = self.time[-1].values

        # Convert the first and last time values to cftime.datetime objects
        first_time = cftime.num2date(
            first_time, calendar=self.calendar, units=self.timeunits
        )
        last_time = cftime.num2date(
            last_time, calendar=self.calendar, units=self.timeunits
        )

        # Calculate the expected start and end dates of the year
        expected_start_date = cftime.datetime(
            first_time.year, 1, 1, 0, 0, 0, calendar=self.calendar
        )
        if self.frequency in ["1hr", "6hr"]:
            expected_end_date = cftime.datetime(
                first_time.year + 1, 1, 1, 0, 0, 0, calendar=self.calendar
            )
        elif self.frequency == "day":
            year = first_time.year + 1
            while str(year)[-1] not in ["1", "6"]:
                year += 1
            expected_end_date = cftime.datetime(
                year, 1, 1, 0, 0, 0, calendar=self.calendar
            )
        else:
            year = first_time.year + 1
            while str(year)[-1] != "1":
                year += 1
            expected_end_date = cftime.datetime(
                year, 1, 1, 0, 0, 0, calendar=self.calendar
            )

        # File chunks as requested by CORDEX-CMIP6
        if self.frequency == "mon":
            nyears = 10
        elif self.frequency == "day":
            nyears = 5
        # subdaily
        else:
            nyears = 1

        # Apply calendar- and frequency-dependent adjustments
        offset = 0
        if self.calendar == "360_day" and self.frequency == "mon":
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

        # Consider experiment end
        exp_last_year = expected_last_sim_year.get(
            self._get_attr("driving_experiment_id", None), None
        )
        if exp_last_year:
            expected_end_date_exp = expected_end_date.replace(year=exp_last_year)
        else:
            expected_end_date_exp = expected_end_date

        if len(messages) == 0:
            errmsg = (
                f"{'Apart from the first and last files of a timeseries ' if nyears>1 else ''}'{nyears}' "
                f"full simulation year{' is' if nyears==1 else 's are'} "
                f"expected in the data file for frequency '{self.frequency}'."
            )
            # Check if the first time is equal to the expected start date
            if first_time != expected_start_date:
                messages.append(
                    f"The first timestep differs from expectation ('{expected_start_date}'): '{first_time}'. "
                    + errmsg
                )
            # Check if the last time is equal to the expected end date
            if last_time != expected_end_date and last_time != expected_end_date_exp:
                messages.append(
                    f"The last timestep differs from expectation ('{expected_end_date}'): '{last_time}'. "
                    + errmsg
                )
        if len(messages) == 0:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_time_range_AS(self, ds):
        """Checks if the time range is as expected."""
        desc = "Time range (Archive Specifications)"
        level = BaseCheck.HIGH
        out_of = 1
        score = 0
        messages = []

        # Check if frequency is known and supported
        # Supported is the intersection of:
        #  CORDEX-CMIP6: fx, 1hr, day, mon
        #  deltdic.keys() - whatever frequencies are defined there
        if self.frequency in ["unknown", "fx"]:
            # Potential error would be raised in base check
            return self.make_result(level, out_of, out_of, desc, messages)
        if self.frequency not in deltdic.keys() or self.frequency not in [
            "1hr",
            "6hr",
            "day",
            "mon",
        ]:
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

        # Get the first and last time values
        first_time = self.time[0].values
        last_time = self.time[-1].values

        # Convert the first and last time values to cftime.datetime objects
        first_time = cftime.num2date(
            first_time, calendar=self.calendar, units=self.timeunits
        )
        last_time = cftime.num2date(
            last_time, calendar=self.calendar, units=self.timeunits
        )

        # Compile the expected time_range
        if self.frequency == "mon":
            time_range = f"{first_time.strftime(format='%4Y%2m')}-{last_time.strftime(format='%4Y%2m')}"
        elif self.frequency == "day":
            time_range = f"{first_time.strftime(format='%4Y%2m%2d')}-{last_time.strftime(format='%4Y%2m%2d')}"
        elif self.frequency in ["1hr", "6hr"]:
            time_range = f"{first_time.strftime(format='%4Y%2m%2d%2H%2M')}-{last_time.strftime(format='%4Y%2m%2d%2H%2M')}"
        else:
            time_range = ""

        # Check if the time_range is as expected
        if self.drs_fn["time_range"] != time_range:
            messages.append(
                f"Expected time_range '{time_range}' but found '"
                f"{self.drs_fn['time_range'] if self.drs_fn['time_range'] else 'unset'}'."
            )
        if len(messages) == 0:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_calendar(self, ds):
        """Checks if the time attributes are as recommended."""
        desc = "Calendar (Archive Specifications)"
        level = BaseCheck.MEDIUM
        out_of = 1
        score = 0
        messages = []

        # Check calendar
        if self.time is not None:
            if self.calendar not in ["standard", "proleptic_gregorian"]:
                messages.append(
                    "Your 'calendar' attribute is not one of the recommended calendars "
                    f"('standard', 'proleptic_gregorian'): '{self.calendar}'."
                )
                if self.calendar == "gregorian":
                    msg = (
                        " Please use the 'standard' calendar, since the use of the 'gregorian' "
                        "calendar is deprecated since CF-1.9."
                    )
                else:
                    msg = " The use of another calendar is OK in case it has been inherited from the driving model."
                messages[-1] = messages[-1] + msg
            else:
                score += 1
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_time_units(self, ds):
        """Checks if the time units are as requested."""
        desc = "Time units (Archive Specifications)"
        level = BaseCheck.HIGH
        out_of = 1
        score = 0
        messages = []

        if self.time is not None:
            if self.timeunits not in [
                "days since 1950-01-01T00:00:00Z",
                "days since 1950-01-01T00:00:00",
                "days since 1950-01-01 00:00:00",
                "days since 1950-01-01",
                "days since 1950-1-1",
                "days since 1850-01-01T00:00:00Z",
                "days since 1850-01-01T00:00:00",
                "days since 1850-01-01 00:00:00",
                "days since 1850-01-01",
                "days since 1850-1-1",
            ]:
                messages.append(
                    "Your time axis' 'units' attribute differs from the allowed values "
                    "('days since 1950-01-01T00:00:00Z', 'days since 1850-01-01T00:00:00Z' "
                    f"if the pre-1950 era is included in the group's simulations): '{self.timeunits}'."
                )
            else:
                score += 1
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_references(self, ds):
        """Checks if references is defined as recommended."""
        desc = "references (Archive Specifications)"
        level = BaseCheck.MEDIUM
        out_of = 1
        score = 0
        messages = []

        if "references" not in self.xrds.attrs or self.xrds.attrs["references"] == "":
            messages.append(
                "The global attribute 'references' is not specified. It is however recommended. "
                "The attribute 'references' should include published or web-based references that describe "
                "the data, model or methods used."
            )
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_version_realization_info(self, ds):
        """Checks if version_realization_info is defined as recommended."""
        desc = "version_realization_info (Archive Specifications)"
        level = BaseCheck.MEDIUM
        out_of = 1
        score = 0
        messages = []

        if (
            any(
                [
                    x != "v1-r1"
                    for x in [
                        self.drs_fn["version_realization"],
                        self.drs_dir["version_realization"],
                        self.drs_gatts["version_realization"],
                    ]
                ]
            )
            and self._get_attr("version_realization_info", default="") == ""
        ):
            messages.append(
                "The global attribute 'version_realization_info' is missing. It is however recommended, "
                "if 'version_realization' deviates from 'v1-r1'. The attribute 'version_realization_info' "
                "provides information on how reruns (eg. v2, v3) and/or realizations (eg. r2, r3) are generated."
            )
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_grid_desc(self, ds):
        """Checks if the global attribute grid is defined as recommended."""
        desc = "grid (description - Archive Specifications)"
        level = BaseCheck.MEDIUM
        out_of = 1
        score = 0
        messages = []

        # Get grid from global attributes - if not defined, another check will throw the error
        grid = self._get_attr("grid", default=False)
        if not grid:
            score += 1
        else:
            # Check if grid description is following the examples
            if re.fullmatch(r"^.* with .* grid spacing.*$", grid):
                score += 1
            else:
                messages.append(
                    "The global attribute 'grid' has no standard form, but it is suggested to include a brief description "
                    "of the native grid and resolution. If the data have been regridded, the regridding procedure and a "
                    "description of the target grid should be provided as well. "
                    "For example: 'Rotated-pole latitude-longitude with 0.22 degree grid spacing'. For a full set of"
                    " examples, please have a look at the CORDEX-CMIP6 Archive Specifications."
                )

        return self.make_result(level, score, out_of, desc, messages)

    def check_version_realization(self, ds):
        """Checks if version_realization is defined as recommended."""
        # todo: can be removed when CV gets updated with a regex for version_realization
        desc = "version_realization (Archive Specifications)"
        level = BaseCheck.HIGH
        out_of = 3
        score = 0
        messages = []

        # Filename
        expected = r"Expected pattern: 'v[1-9]\d*-r[1-9]\d*', eg. 'v1-r3'."
        if self.drs_fn["version_realization"]:
            if not bool(
                re.fullmatch(
                    r"v[1-9]\d*-r[1-9]\d*",
                    self.drs_fn["version_realization"],
                    flags=re.ASCII,
                )
            ):
                messages.append(
                    f"DRS filename building block 'version_realization' does not comply with the CORDEX-CMIP6 Archive Specifications: '{self.drs_fn['version_realization']}'. {expected}"
                )
            else:
                score += 1
        else:
            messages.append(
                "DRS filename building block 'version_realization' is missing."
            )

        # Folder structure
        if self.drs_dir["version_realization"]:
            if not bool(
                re.fullmatch(
                    r"v[1-9]\d*-r[1-9]\d*",
                    self.drs_dir["version_realization"],
                    flags=re.ASCII,
                )
            ):
                messages.append(
                    f"DRS path building block 'version_realization' does not comply with the CORDEX-CMIP6 Archive Specifications: '{self.drs_dir['version_realization']}'. {expected}"
                )
            else:
                score += 1
        else:
            messages.append("DRS path building block 'version_realization' is missing.")

        # Global attribute
        if self.drs_gatts["version_realization"]:
            if not bool(
                re.fullmatch(
                    r"v[1-9]\d*-r[1-9]\d*",
                    self.drs_gatts["version_realization"],
                    flags=re.ASCII,
                )
            ):
                messages.append(
                    f"Global attribute 'version_realization' does not comply with the CORDEX-CMIP6 Archive Specifications: '{self.drs_gatts['version_realization']}'. {expected}"
                )
            else:
                score += 1
        else:
            messages.append("Global attribute 'version_realization' is missing.")

        return self.make_result(level, score, out_of, desc, messages)

    def check_driving_attributes(self, ds):
        """Checks if all driving attributes are defined as required."""
        desc = "Driving attributes (Archive Specifications)"
        level = BaseCheck.HIGH
        out_of = 3
        score = 0
        messages = []

        dei = self._get_attr("driving_experiment_id", False)
        dvl = self._get_attr("driving_variant_label", False)
        dsi = self._get_attr("driving_source_id", False)

        if dvl and dvl == "r0i0p0f0":
            messages.append(
                "The global attribute 'driving_variant_label' is not compliant with the archive specifications "
                f"('r1i1p1f1' is the minimum 'driving_variant_label'): '{dvl}'."
            )
        else:
            score += 1

        if dei and dei == "evaluation":
            if dvl and dvl != "r1i1p1f1":
                messages.append(
                    "The global attribute 'driving_variant_label' is not compliant with the archive specifications "
                    f"('r1i1p1f1'): '{dei}'."
                )
            else:
                score += 1
            if dsi and dsi != "ERA5":
                messages.append(
                    "The global attribute 'driving_source_id' is not compliant with the archive specifications "
                    f"('ERA5'): '{dei}'."
                )
            else:
                score += 1
        else:
            score += 2

        return self.make_result(level, score, out_of, desc, messages)

    def check_driving_source(self, ds):
        """Checks if global attribute driving_source is (correctly) defined."""
        desc = "driving_source (CV)"
        out_of = 2
        score = 0
        messages = []
        # Level varies
        # - if driving_source is not defined, it is recommended to define it
        # - if it is incorrectly defined, it is required to correct it

        # Test if driving_source is defined
        drivs = self._get_attr("driving_source", False)
        if not drivs:
            level = BaseCheck.MEDIUM
            messages = [
                "The global attribute 'driving_source' is not defined. It is however recommended to include it."
            ]
            return self.make_result(level, score, out_of, desc, messages)
        else:
            level = BaseCheck.HIGH
            score += 1
            drivsid = self._get_attr("driving_source_id", False)
            # Abort if driving_source_id undefined or unknown (will cause a failed check elsewhere)
            if not drivsid or drivsid not in self.CV["driving_source_id"]:
                return self.make_result(level, out_of, out_of, desc, messages)
            # Else compare with CV
            elif drivs != self.CV["driving_source_id"][drivsid]["driving_source"]:
                messages = [
                    "The global attribute 'driving_source' does not comply with the CV."
                ]
            else:
                score += 1

            return self.make_result(level, score, out_of, desc, messages)

    def check_grid_mapping(self, ds):
        """Checks if the grid_mapping label is compliant with the archive specifications."""
        desc = "grid_mapping label (Archive Specifications)"
        level = BaseCheck.HIGH
        out_of = 5
        score = 0
        messages = []

        grid_mapping_name = False

        # The allowed grid_mapping_name attribute values in CORDEX-CMIP6
        gmallowed = ["lambert_conformal_conic", "rotated_latitude_longitude"]
        # One of the following attributes needs to be specified for the grid_mapping variable
        # assuming that means that the Earth is specified/described as requested
        # (the checking of the validity of the description is left to CF checks)
        gmoptattrs = ["earth_radius", "semi_major_axis"]
        if len(self.varname) > 0:
            crs = getattr(ds.variables[self.varname[0]], "grid_mapping", False)
            if crs:
                grid_mapping_name = getattr(
                    ds.variables[crs], "grid_mapping_name", False
                )
                # Check grid_mapping label
                if grid_mapping_name and crs in ["crs", grid_mapping_name]:
                    score += 1
                else:
                    messages.append(
                        f"The grid_mapping label '{crs}' needs to be either 'crs'"
                        " or equal to the grid_mapping_name (eg. 'rotated_latitude_longitude')."
                    )
                # Check grid_mapping_name
                if grid_mapping_name and grid_mapping_name in gmallowed:
                    score += 1
                else:
                    messages.append(
                        f"The grid_mapping_name '{grid_mapping_name}' must be one of:"
                        f""" {", ".join(["'" + gm  + "'" for gm in gmallowed])}."""
                    )
                # Check presence of description of spherical / ellipsoid Earth
                # - leave actual checking of the validity of that info to CF
                if any(
                    [getattr(ds.variables[crs], attr, False) for attr in gmoptattrs]
                ):
                    score += 1
                else:
                    messages.append(
                        f"The grid_mapping variable '{crs}' needs to include information regarding"
                        " the shape and size of the Earth used for the model grid. See 'CF-1.11 Appendix F'"
                        " of the CF-Conventions for further information."
                    )
                # Check data type of grid_mapping variable (int or char)
                if ds[crs].dtype == np.int32 or ds[crs].dtype.kind == "S":
                    score += 1
                else:
                    messages.append(
                        f"The grid_mapping variable '{crs}' needs to be of type 'int' or 'char', "
                        f"but is of type '{ds[crs].dtype} ({ds[crs].dtype.kind})'."
                    )
            else:
                messages.append(
                    f"The grid_mapping variable '{crs}', describing the coordinate reference system,"
                    " could not be found in the file."
                )

        else:
            score += 4
        # rlat, rlon or y, x must be present in file, depending on the grid_mapping_name
        if grid_mapping_name and grid_mapping_name in gmallowed:
            if grid_mapping_name == "lambert_conformal_conic":
                if "y" not in self.xrds or "x" not in self.xrds:
                    messages.append(
                        "The grid_mapping_name 'lambert_conformal_conic' requires the variables"
                        " 'y' and 'x' to be present in the file defining the native coordinate"
                        " system used by the RCM."
                    )
                else:
                    score += 1
            else:
                if "rlat" not in self.xrds or "rlon" not in self.xrds:
                    messages.append(
                        "The grid_mapping_name 'rotated_latitude_longitude' requires the variables"
                        " 'rlat' and 'rlon' to be present in the file defining the native"
                        " coordinate system used by the RCM."
                    )
                else:
                    score += 1
        else:
            if ("rlat" in self.xrds and "rlon" in self.xrds) or (
                "y" in self.xrds and "x" in self.xrds
            ):
                score += 1
            else:
                messages.append(
                    "The variables 'rlat', 'rlon' or 'y' and 'x' need to be present in the"
                    " file defining the native coordinate system used by the RCM."
                )

        return self.make_result(level, score, out_of, desc, messages)

    def check_lon_value_range(self, ds):
        """Checks if lon values are within the required range."""
        desc = "Longitude value range (Archive Specifications)"
        level = BaseCheck.HIGH
        out_of = 3
        score = 0
        messages = []

        if "longitude" in self.xrds.cf.coordinates:
            lon = self.xrds[self.xrds.cf.coordinates["longitude"][0]]
        elif "lon" in self.xrds:
            lon = self.xrds["lon"]
        else:
            return self.make_result(level, out_of, out_of, desc, messages)

        # Check if longitude coordinates are strictly monotonically increasing
        if lon.ndim != 2:
            messages.append("The longitude coordinate should have two dimensions.")
        else:
            increasing_0 = ((lon[1:, :].data - lon[:-1, :].data) > 0).all()
            increasing_1 = ((lon[:, 1:].data - lon[:, :-1].data) > 0).all()
            if "X" in self.xrds.cf.axes:
                rlon_idx = lon.dims.index(self.xrds.cf.axes["X"][0])
                if rlon_idx == 0:
                    if increasing_0:
                        score += 1
                    else:
                        messages.append(
                            "The longitude coordinate should be strictly monotonically increasing."
                            f"{increasing_0}, {increasing_1}"
                        )
                elif rlon_idx == 1:
                    if increasing_1:
                        score += 1
                    else:
                        messages.append(
                            "The longitude coordinate should be strictly monotonically increasing."
                            f"{increasing_0}, {increasing_1}"
                        )
            elif increasing_0 or increasing_1:
                score += 1
            else:
                messages.append(
                    "The longitude coordinate should be strictly monotonically increasing."
                    f"{increasing_0}, {increasing_1}"
                )

        # Check if longitude coordinates are confined to the range -180 to 360
        in_range = (lon >= -180).all() and (lon <= 360).all()
        if in_range:
            score += 1
        else:
            messages.append(
                "Longitude coordinates should be confined to the range -180 to 360."
            )

        # Check if longitude coordinates have absolute values as small as possible
        abs = (lon > 180).any() and (xr.where(lon >= 180, lon - 360, lon) >= -180).all()
        if not abs:
            score += 1
        else:
            messages.append(
                "Longitude values are required to take the smalles absolute value in the range [-180, 360]."
            )

        return self.make_result(level, score, out_of, desc, messages)

    def check_lat_lon_bounds(self, ds):
        """Checks if lat and lon bounds are present"""
        desc = "Presence of latitude and longitude bounds (Archive Specifications)"
        level = BaseCheck.MEDIUM
        out_of = 1
        score = 0
        messages = []

        if "longitude" in self.xrds.cf.bounds and "latitude" in self.xrds.cf.bounds:
            score += 1
        elif ("lat_bnds" in self.xrds and "lon_bnds" in self.xrds) or (
            "vertices_lat" in self.xrds and "vertices_lon" in self.xrds
        ):
            score += 1
        else:
            messages.append(
                "It is recommended for the variables 'lat' and 'lon' to have bounds defined."
            )
        return self.make_result(level, score, out_of, desc, messages)

    def check_horizontal_axes_bounds(self, ds):
        """Checks if rlat/rlon bounds or x/y bounds are present"""
        desc = "Presence of horizontal axes bounds (Archive Specifications)"
        level = BaseCheck.MEDIUM
        out_of = 1
        score = 0
        messages = []

        if "X" in self.xrds.cf.bounds and "Y" in self.xrds.cf.bounds:
            score += 1
        elif ("rlat_bnds" in self.xrds and "rlon_bnds" in self.xrds) or (
            "x_bnds" in self.xrds and "y_bnds" in self.xrds
        ):
            score += 1
        else:
            messages.append(
                "It is recommended for the variables 'rlat' and 'rlon' or 'x' and 'y' to have bounds defined."
            )
        return self.make_result(level, score, out_of, desc, messages)

    def check_domain_id(self, ds):
        """Checks if the domain_id is compliant with the archive specifications."""
        desc = "domain_id (CV)"
        level = BaseCheck.HIGH
        out_of = 2
        score = 0
        messages = []

        # Get domain_id from global attributes
        domain_id = self._get_attr("domain_id", default=False)

        # Do not give a result if not defined
        if not domain_id:
            return self.make_result(level, out_of, out_of, desc, messages)

        # If the grid is rectilinear, the domain_id needs to include the suffix "i"
        try:
            lat = self.xrds.cf.coordinates["latitude"][0]
            lon = self.xrds.cf.coordinates["longitude"][0]
        except KeyError:
            messages.append(
                "Cannot check 'domain_id' as latitude and longitude coordinate variables could not be identified."
            )
            return self.make_result(level, score, out_of, desc, messages)

        # Rectilinear case: lat and lon must be 1D
        #  (would also be the case for unstructured grids, but those are not allowed in CORDEX-CMIP6)
        if self.xrds[lat].ndim == 1 and self.xrds[lon].ndim == 1:
            if not domain_id.endswith("i"):
                messages.append(
                    "The global attribute 'domain_id' is not compliant with the archive specifications "
                    f"('domain_id' should get the suffix 'i' in case of a rectilinear grid): '{domain_id}'."
                )
            else:
                score += 1
                domain_id = domain_id[:-1]
        else:
            score += 1

        # Check if domain_id is in the CV
        if domain_id not in self.CV["domain_id"]:
            messages.append(
                f"The global attribute 'domain_id' is not compliant with the CV: '{domain_id}'."
            )
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)

    def check_institution(self, ds):
        """Checks if the institution is compliant with the CV."""
        desc = "institution (CV)"
        level = BaseCheck.HIGH
        out_of = 1
        score = 0
        messages = []

        # Get institution from global attributes
        institution = self._get_attr("institution", default=False)
        institution_id = self._get_attr("institution_id", default=False)

        # If check cannot be conducted, rely on base checks
        if (
            not institution_id
            or not institution
            or institution_id not in self.CV["institution_id"]
        ):
            return self.make_result(level, out_of, out_of, desc, messages)

        # Check institution against CV
        if institution != self.CV["institution_id"][institution_id]:
            messages.append(
                f"The global attribute 'institution' is not compliant with the CV:"
                f" '{institution}' instead of '{self.CV['institution_id'][institution_id]}'."
            )
        else:
            score += 1

        return self.make_result(level, score, out_of, desc, messages)
