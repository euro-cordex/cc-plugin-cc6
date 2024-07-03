import os
import re
from datetime import timedelta

import cf_xarray  # noqa
import cftime
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
        if not self.inputs.get("tables", False):
            if self.debug:
                print("Downloading CV and CMOR tables.")
            tables_path = "~/.cc6_metadata/cordex-cmip6-cmor-tables"
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
                )
                if os.path.basename(os.path.realpath(filename_retrieved)) != filename:
                    raise AssertionError(
                        f"Download failed for CV table '{filename_retrieved}' (source: '{url}')."
                    )

            self._initialize_CV_info(tables_path)
            self._initialize_time_info()
            self._initialize_coords_info()

        # Specify the global attributes that will be checked by a specific check
        #  rather than a general check against the value given in the CV
        #  (i.e. because it does not explicitly defined in the CV)
        self.global_attrs_hard_checks = [
            "variable_id",
            "time_range",
            "version",
            "source",
            "domain_id",
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
                f"File format differs from expectation ({data_model_expected}/{disk_format_expected}): '{ds.data_model}/{ds.disk_format}'."
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
            first_time.year, 1, 1, 0, 0, 0, calendar=self.calendar
        )
        expected_end_date = cftime.datetime(
            last_time.year + nyears, 1, 1, 0, 0, 0, calendar=self.calendar
        )

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

        if len(messages) == 0:
            errmsg = (
                f"{'Apart from the last file of a timeseries ' if nyears>1 else ''}'{nyears}' full simulation year{' is' if nyears==1 else 's are'} "
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
                f"Expected time_range '{time_range}' but found '{self.drs_fn['time_range'] if self.drs_fn['time_range'] else 'unset'}'."
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
                    f"Your 'calendar' attribute is not one of the recommended calendars ('standard', 'proleptic_gregorian'): '{self.calendar}'."
                )
                if self.calendar == "gregorian":
                    msg = " Please use the 'standard' calendar, since the use of the 'gregorian' calendar is deprecated since CF-1.9."
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
                "days since 1850-01-01T00:00:00Z",
            ]:
                messages.append(
                    f"Your time axis' 'units' attribute differs from the allowed values ('days since 1950-01-01T00:00:00Z', 'days since 1850-01-01T00:00:00Z' if the pre-1950 era is included in the group's simulations.): '{self.timeunits}'."
                )
            else:
                score += 1
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
                "The global attribute 'version_realization_info' is missing. It is however recommended, if 'version_realization' deviates from 'v1-r1'. The attribute 'version_realization_info' provides information on how reruns (eg. v2, v3) and/or realizations (eg. r2, r3) are generated."
            )
        else:
            score += 1

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
                f"The global attribute 'driving_variant_label' is not compliant with the archive specifications ('r1i1p1f1' is the minimum 'driving_variant_label'): '{dvl}'."
            )
        else:
            score += 1

        if dei and dei == "evaluation":
            if dvl and dvl != "r1i1p1f1":
                messages.append(
                    f"The global attribute 'driving_variant_label' is not compliant with the archive specifications ('r1i1p1f1'): '{dei}'."
                )
            else:
                score += 1
            if dsi and dsi != "ERA5":
                messages.append(
                    f"The global attribute 'driving_source_id' is not compliant with the archive specifications ('ERA5'): '{dei}'."
                )
            else:
                score += 1
        else:
            score += 2

        return self.make_result(level, score, out_of, desc, messages)

    def check_domain_id(self, ds):
        """Checks if the domain_id is compliant with the archive specifications."""
        desc = "domain_id (CV)"
        level = BaseCheck.MEDIUM
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
                    f"The global attribute 'domain_id' is not compliant with the archive specifications ('domain_id' should get the suffix 'i' in case of a rectilinear grid): '{domain_id}'."
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
