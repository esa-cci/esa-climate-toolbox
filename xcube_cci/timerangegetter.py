# The MIT License (MIT)
# Copyright (c) 2023 ESA Climate Change Initiative
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from typing import Any
from typing import List
from typing import Mapping
from typing import Tuple
from typing import Union

from xcube_cci.ccicdc import CciCdc

from xcube_cci.constants import MONTHS
from .constants import TIMESTAMP_FORMAT


def extract_time_range_as_strings(
        time_range: Union[Tuple, List]
) -> (str, str):
    if isinstance(time_range, tuple):
        time_start, time_end = time_range
    else:
        time_start = time_range[0]
        time_end = time_range[1]
    return \
        extract_time_as_string(time_start), \
        extract_time_as_string(time_end)


def extract_time_as_string(
        time_value: Union[pd.Timestamp, str]
) -> str:
    if isinstance(time_value, str):
        time_value = pd.to_datetime(time_value, utc=True)
    return time_value.tz_localize(None).isoformat()


class TimeRangeGetter:

    def __init__(self, cci_cdc: CciCdc, metadata: dict):
        self._cci_cdc = cci_cdc
        self._metadata = metadata

    def get_time_ranges(self, dataset_id: str,
                        params: Mapping[str, Any]) -> List[Tuple]:
        start_time, end_time, iso_start_time, iso_end_time = \
            self._extract_time_range_as_datetime(
                params.get('time_range',
                           self.get_default_time_range(dataset_id)))
        time_period = dataset_id.split('.')[2]
        if time_period == 'day':
            start_time = datetime(year=start_time.year, month=start_time.month,
                                  day=start_time.day)
            end_time = datetime(year=end_time.year, month=end_time.month,
                                day=end_time.day,
                                hour=23, minute=59, second=59)
            delta = relativedelta(days=1)
        elif time_period == 'month' or time_period == 'mon':
            start_time = datetime(year=start_time.year, month=start_time.month,
                                  day=1)
            end_time = datetime(year=end_time.year, month=end_time.month, day=1)
            delta = relativedelta(months=1)
            end_time += delta
        elif time_period == 'year' or time_period == 'yr':
            start_time = datetime(year=start_time.year, month=1, day=1)
            end_time = datetime(year=end_time.year, month=12, day=31)
            delta = relativedelta(years=1)
            if dataset_id.endswith("yr"):
                num_years = int(dataset_id[-3:-2])
                delta_years = relativedelta(years=num_years) - relativedelta(days=1)
                request_time_ranges = []
                this = start_time
                after = this + delta_years
                while after <= end_time:
                    pd_this = pd.Timestamp(datetime.strftime(this, TIMESTAMP_FORMAT))
                    pd_next = pd.Timestamp(datetime.strftime(after, TIMESTAMP_FORMAT))
                    request_time_ranges.append((pd_this, pd_next))
                    this = this + delta
                    after = after + delta
                return request_time_ranges
        elif time_period == 'climatology':
            return [(i + 1, i + 1) for i, month in enumerate(MONTHS)]
        else:
            end_time = end_time.replace(hour=23, minute=59, second=59)
            end_time_str = datetime.strftime(end_time, TIMESTAMP_FORMAT)
            iso_end_time = extract_time_as_string(end_time_str)
            request_time_ranges = self._cci_cdc.get_time_ranges_from_data(
                dataset_id, iso_start_time, iso_end_time)
            return request_time_ranges
        request_time_ranges = []
        this = start_time
        while this < end_time:
            after = this + delta
            pd_this = pd.Timestamp(datetime.strftime(this, TIMESTAMP_FORMAT))
            pd_next = pd.Timestamp(datetime.strftime(after, TIMESTAMP_FORMAT))
            request_time_ranges.append((pd_this, pd_next))
            this = after
        return request_time_ranges

    @staticmethod
    def _extract_time_range_as_datetime(
            time_range: Union[Tuple, List]
    ) -> (datetime, datetime, str, str):
        iso_start_time, iso_end_time = extract_time_range_as_strings(
            time_range)
        start_time = datetime.strptime(iso_start_time, TIMESTAMP_FORMAT)
        end_time = datetime.strptime(iso_end_time, TIMESTAMP_FORMAT)
        return start_time, end_time, iso_start_time, iso_end_time

    def get_default_time_range(self, ds_id: str):
        temporal_start = self._metadata.get('temporal_coverage_start', None)
        temporal_end = self._metadata.get('temporal_coverage_end', None)
        if not temporal_start or not temporal_end:
            time_ranges = self._cci_cdc.get_time_ranges_from_data(ds_id)
            if not temporal_start:
                if len(time_ranges) == 0:
                    raise ValueError(
                        "Could not determine temporal start of dataset. "
                        "Please use 'time_range' parameter."
                    )
                temporal_start = time_ranges[0][0]
            if not temporal_end:
                if len(time_ranges) == 0:
                    raise ValueError(
                        "Could not determine temporal end of dataset. "
                        "Please use 'time_range' parameter."
                    )
                temporal_end = time_ranges[-1][1]
        return temporal_start, temporal_end
