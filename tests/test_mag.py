# Copyright 2020 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: James Diprose


import os
import unittest

import pendulum

from mag_archiver.mag import MagArchiverClient, make_mag_query, MagState, MagDateType


class TestMag(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestMag, self).__init__(*args, **kwargs)

        self.account_name = os.getenv('STORAGE_ACCOUNT_NAME')
        self.account_key = os.getenv('STORAGE_ACCOUNT_KEY')

    def test_list_containers(self):
        # TODO: mock Azure functions
        client = MagArchiverClient(account_name=self.account_name, account_key=self.account_key)
        containers = client.list_containers(last_modified_thresh=1)
        self.assertEqual(len(containers), 39)

    def test_update_releases(self):
        # TODO: mock Azure functions ?
        client = MagArchiverClient(account_name=self.account_name, account_key=self.account_key)
        containers = client.list_containers(last_modified_thresh=1)
        num_updated, num_errors = client.update_releases(containers)
        self.assertEqual(num_updated, 0)
        self.assertEqual(num_errors, 0)

    def test_make_mag_query(self):
        start_date = pendulum.datetime(year=2020, month=4, day=1)
        end_date = pendulum.datetime(year=2020, month=5, day=1)

        # No parameters
        query = make_mag_query()
        self.assertEqual(query, '')

        # State parameter
        query = make_mag_query(state=MagState.discovered)
        self.assertEqual(query, "State eq 'discovered'")

        query = make_mag_query(state=MagState.archived)
        self.assertEqual(query, "State eq 'archived'")

        query = make_mag_query(state=MagState.done)
        self.assertEqual(query, "State eq 'done'")

        # Start date parameter
        query = make_mag_query(start_date=start_date, date_type=MagDateType.release)
        self.assertEqual(query, "ReleaseDate ge datetime'2020-04-01T00:00Z'")

        query = make_mag_query(start_date=start_date, date_type=MagDateType.discovered)
        self.assertEqual(query, "DiscoveredDate ge datetime'2020-04-01T00:00Z'")

        query = make_mag_query(start_date=start_date, date_type=MagDateType.archived)
        self.assertEqual(query, "ArchivedDate ge datetime'2020-04-01T00:00Z'")

        query = make_mag_query(start_date=start_date, date_type=MagDateType.done)
        self.assertEqual(query, "DoneDate ge datetime'2020-04-01T00:00Z'")

        # End date parameter
        query = make_mag_query(end_date=end_date, date_type=MagDateType.release)
        self.assertEqual(query, "ReleaseDate lt datetime'2020-05-01T00:00Z'")

        query = make_mag_query(end_date=end_date, date_type=MagDateType.discovered)
        self.assertEqual(query, "DiscoveredDate lt datetime'2020-05-01T00:00Z'")

        query = make_mag_query(end_date=end_date, date_type=MagDateType.archived)
        self.assertEqual(query, "ArchivedDate lt datetime'2020-05-01T00:00Z'")

        query = make_mag_query(end_date=end_date, date_type=MagDateType.done)
        self.assertEqual(query, "DoneDate lt datetime'2020-05-01T00:00Z'")

        # Start date, end date and date type
        query = make_mag_query(start_date=start_date, end_date=end_date, date_type=MagDateType.release)
        self.assertEqual(query, "ReleaseDate ge datetime'2020-04-01T00:00Z' and ReleaseDate lt "
                                "datetime'2020-05-01T00:00Z'")

        query = make_mag_query(start_date=start_date, end_date=end_date, date_type=MagDateType.discovered)
        self.assertEqual(query, "DiscoveredDate ge datetime'2020-04-01T00:00Z' and DiscoveredDate lt "
                                "datetime'2020-05-01T00:00Z'")

        query = make_mag_query(start_date=start_date, end_date=end_date, date_type=MagDateType.archived)
        self.assertEqual(query, "ArchivedDate ge datetime'2020-04-01T00:00Z' and ArchivedDate lt "
                                "datetime'2020-05-01T00:00Z'")

        query = make_mag_query(start_date=start_date, end_date=end_date, date_type=MagDateType.done)
        self.assertEqual(query, "DoneDate ge datetime'2020-04-01T00:00Z' and DoneDate lt "
                                "datetime'2020-05-01T00:00Z'")

        # State, start date, end date and date type
        query = make_mag_query(state=MagState.discovered, start_date=start_date, end_date=end_date,
                               date_type=MagDateType.discovered)
        self.assertEqual(query, "State eq 'discovered' and DiscoveredDate ge datetime'2020-04-01T00:00Z' "
                                "and DiscoveredDate lt datetime'2020-05-01T00:00Z'")

        query = make_mag_query(state=MagState.archived, start_date=start_date, end_date=end_date,
                               date_type=MagDateType.archived)
        self.assertEqual(query, "State eq 'archived' and ArchivedDate ge datetime'2020-04-01T00:00Z' "
                                "and ArchivedDate lt datetime'2020-05-01T00:00Z'")

        query = make_mag_query(state=MagState.done, start_date=start_date, end_date=end_date,
                               date_type=MagDateType.done)
        self.assertEqual(query, "State eq 'done' and DoneDate ge datetime'2020-04-01T00:00Z' "
                                "and DoneDate lt datetime'2020-05-01T00:00Z'")

    def test_list_releases(self):
        # TODO: mock Azure functions

        client = MagArchiverClient(account_name=self.account_name, account_key=self.account_key)
        start_date = pendulum.datetime(year=2020, month=4, day=1)
        end_date = pendulum.datetime(year=2020, month=5, day=1)
        releases = client.list_releases(start_date=start_date, end_date=end_date, state=MagState.discovered,
                                        date_type=MagDateType.release)
        self.assertEqual(len(releases), 4)
