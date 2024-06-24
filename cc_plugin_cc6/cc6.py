import os
import re
from datetime import timedelta

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
            tables_path = "~/.cc_metadata/cordex-cmip6-cmor-tables"
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
