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
from unittest.mock import patch

import pendulum
from azure.common import AzureMissingResourceHttpError
from azure.cosmosdb.table.tableservice import TableService
from azure.storage.blob import ContainerProperties

from mag_archiver.azure import create_table
from mag_archiver.mag import make_mag_query, MagState, MagDateType, MagRelease, MagTask, MagArchiverClient, \
    hide_if_not_none


class TestMag(unittest.TestCase):

    def test_hide_if_not_none(self):
        # Test that None is returned for None
        value = hide_if_not_none(None)
        self.assertEqual(value, None)

        # Test that 'hidden' is returned: string
        value = hide_if_not_none('hello world')
        self.assertEqual(value, 'hidden')

        # Test that 'hidden' is returned: integer
        value = hide_if_not_none(123)
        self.assertEqual(value, 'hidden')

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


def make_mag_release(account_name: str, account_key: str, year: int, month: int, day: int):
    min_date = pendulum.datetime(1601, 1, 1)
    partition_key_ = 'mag'
    row_key_ = f'mag-{year:0>4d}-{month:0>2d}-{day:0>2d}'
    state_ = MagState.discovered
    task_ = MagTask.not_started
    release_date_ = pendulum.datetime(year=year, month=month, day=day)
    source_container_ = row_key_
    source_container_last_modified_ = pendulum.datetime(year=year, month=month, day=day, hour=1)
    release_container_ = ''
    release_path_ = ''
    discovered_date_ = pendulum.datetime(year=year, month=month, day=day, hour=2)
    archived_date_ = min_date
    done_date_ = min_date
    return MagRelease(partition_key_, row_key_, state_, task_, release_date_, source_container_,
                      source_container_last_modified_, release_container_, release_path_, discovered_date_,
                      archived_date_, done_date_, account_name=account_name, account_key=account_key)


class TestMagRelease(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestMagRelease, self).__init__(*args, **kwargs)
        self.account_name = os.getenv('TEST_AZURE_STORAGE_ACCOUNT_NAME')
        self.account_key = os.getenv('TEST_AZURE_STORAGE_ACCOUNT_KEY')
        create_table(self.account_name, self.account_key, MagRelease.TABLE_NAME)

    def test_secrets_hidden(self):
        # Check that account key is hidden
        account_name = 'myaccountname'
        secret = 'secret'

        # Check that account_key and sas_token are hidden
        release = make_mag_release(account_name, secret, 2020, 1, 1)
        self.assertIn('account_key=hidden', release.__repr__())
        self.assertNotIn(secret, release.__str__())
        self.assertNotIn(secret, release.__repr__())

        # Check that account_key is None
        release = make_mag_release(account_name, None, 2020, 1, 1)
        self.assertIn('account_key=None', release.__repr__())

    def test_create(self):
        release = make_mag_release(self.account_name, self.account_key, 2019, 6, 1)
        try:
            success = release.create()
            self.assertTrue(success)
        finally:
            release.delete()

    def test_delete(self):
        release = make_mag_release(self.account_name, self.account_key, 2019, 6, 1)

        # Check that we can create and then delete
        release.create()
        release.delete()

        # Check that second delete fails
        with self.assertRaises(AzureMissingResourceHttpError):
            release.delete()

    def test_update(self):
        release = make_mag_release(self.account_name, self.account_key, 2019, 6, 1)
        try:
            release.create()

            # Update release
            release.state = MagState.archived
            release.archived_date = pendulum.now('UTC').microsecond
            release.update()

            # Verify that release is updated
            service = TableService(account_name=self.account_name, account_key=self.account_key)
            entity = service.get_entity(MagRelease.TABLE_NAME, release.partition_key, release.row_key)
            updated_release = MagRelease.from_entity(entity)
            self.assertEqual(release.state, updated_release.state)
            self.assertEqual(release.archived_date, updated_release.archived_date)
        finally:
            release.delete()


def make_containers():
    containers = []
    cp1 = ContainerProperties()
    cp1.name = 'mag-2020-04-17'
    cp1.last_modified = pendulum.datetime(year=2020, month=4, day=18)
    containers.append(cp1)

    cp3 = ContainerProperties()
    cp3.name = 'mag-2020-05-01'
    cp3.last_modified = pendulum.datetime(year=2020, month=5, day=1)
    containers.append(cp3)

    cp2 = ContainerProperties()
    cp2.name = 'mag-2020-04-24'
    cp2.last_modified = pendulum.datetime(year=2020, month=4, day=25)
    containers.append(cp2)

    return containers


class TestMagArchiverClient(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestMagArchiverClient, self).__init__(*args, **kwargs)
        self.account_name = os.getenv('TEST_AZURE_STORAGE_ACCOUNT_NAME')
        self.account_key = os.getenv('TEST_AZURE_STORAGE_ACCOUNT_KEY')
        create_table(self.account_name, self.account_key, MagRelease.TABLE_NAME)

    def test_secrets_hidden(self):
        # Check that account key is hidden
        account_name = 'myaccountname'
        secret = 'secret'

        # Check that account_key and sas_token are hidden
        client = MagArchiverClient(account_name=account_name, account_key=secret, sas_token=secret)
        expected = f'MagArchiverClient(account_name={account_name}, account_key=hidden, sas_token=hidden)'
        self.assertEqual(client.__str__(), expected)
        self.assertEqual(client.__repr__(), expected)
        self.assertNotIn(secret, client.__str__())
        self.assertNotIn(secret, client.__repr__())

        # Check that account_key and sas_token are None
        client = MagArchiverClient(account_name=account_name)
        expected = f'MagArchiverClient(account_name={account_name}, account_key=None, sas_token=None)'
        self.assertEqual(client.__str__(), expected)
        self.assertEqual(client.__repr__(), expected)

    @patch('mag_archiver.mag.list_containers')
    @patch('pendulum.datetime.now')
    def test_list_containers(self, mock_now, mock_list_containers):
        # Mock time
        mock_now.return_value = pendulum.datetime(year=2020, month=5, day=1, minute=10)

        # Mock containers
        containers_in = make_containers()
        mock_list_containers.return_value = containers_in

        # Test that 2 containers are returned when last_modified_thresh=1
        client = MagArchiverClient(account_name=self.account_name, account_key=self.account_key)
        containers_out = client.list_containers(last_modified_thresh=1)
        self.assertEqual(len(containers_out), 2)

        # Test that 3 containers are returned when last_modified_thresh=0
        containers_out = client.list_containers(last_modified_thresh=0)
        self.assertEqual(len(containers_out), 3)

        # Test sort order reverse=False
        self.assertEqual(containers_in[0].name, containers_out[0].name)
        self.assertEqual(containers_in[2].name, containers_out[1].name)
        self.assertEqual(containers_in[1].name, containers_out[2].name)

        # Test sort order reverse=True
        containers_out = client.list_containers(last_modified_thresh=0, reverse=True)
        self.assertEqual(len(containers_out), 3)
        self.assertEqual(containers_in[1].name, containers_out[0].name)
        self.assertEqual(containers_in[2].name, containers_out[1].name)
        self.assertEqual(containers_in[0].name, containers_out[2].name)

    @patch('mag_archiver.mag.list_containers')
    @patch('pendulum.datetime.now')
    def test_update_releases(self, mock_now, mock_list_containers):
        # Mock time
        mock_now.return_value = pendulum.datetime(year=2020, month=5, day=1, minute=10)

        # Mock containers
        containers_in = make_containers()
        mock_list_containers.return_value = containers_in

        # Mock fetching of containers
        client = MagArchiverClient(account_name=self.account_name, account_key=self.account_key)
        containers = client.list_containers(last_modified_thresh=1)

        try:
            # Update releases based on containers
            num_updated, num_errors = client.update_releases(containers)
            self.assertEqual(num_updated, 2)
            self.assertEqual(num_errors, 0)
        finally:
            # Clean up
            service = TableService(account_name=self.account_name, account_key=self.account_key)
            for container in containers:
                service.delete_entity(MagRelease.TABLE_NAME, 'mag', container.name.replace("mag-", ""))

    @patch('mag_archiver.mag.list_containers')
    @patch('pendulum.datetime.now')
    def test_list_releases(self, mock_now, mock_list_containers):
        # Mock time
        mock_now.return_value = pendulum.datetime(year=2020, month=5, day=1, hour=1)

        # Mock containers
        containers_in = make_containers()
        mock_list_containers.return_value = containers_in

        # Mock fetching of containers
        client = MagArchiverClient(account_name=self.account_name, account_key=self.account_key)
        containers = client.list_containers(last_modified_thresh=1)

        try:
            # Update releases based on containers
            num_updated, num_errors = client.update_releases(containers)
            self.assertEqual(num_updated, 3)
            self.assertEqual(num_errors, 0)

            # Two releases
            start_date = pendulum.datetime(year=2020, month=4, day=17)
            end_date = pendulum.datetime(year=2020, month=5, day=1)
            releases = client.list_releases(start_date=start_date, end_date=end_date, state=MagState.discovered,
                                            date_type=MagDateType.release)
            self.assertEqual(len(releases), 2)

            # 1 release
            start_date = pendulum.datetime(year=2020, month=4, day=17, minute=1)
            end_date = pendulum.datetime(year=2020, month=5, day=1)
            releases = client.list_releases(start_date=start_date, end_date=end_date, state=MagState.discovered,
                                            date_type=MagDateType.release)
            self.assertEqual(len(releases), 1)

            # Three releases
            start_date = pendulum.datetime(year=2020, month=4, day=17)
            end_date = pendulum.datetime(year=2020, month=5, day=1, minute=1)
            releases = client.list_releases(start_date=start_date, end_date=end_date, state=MagState.discovered,
                                            date_type=MagDateType.release, reverse=False)
            self.assertEqual(len(releases), 3)

            # Sorting reverse=False
            self.assertEqual(releases[0].row_key, '2020-04-17')
            self.assertEqual(releases[1].row_key, '2020-04-24')
            self.assertEqual(releases[2].row_key, '2020-05-01')

            # Sorting reverse=True
            releases = client.list_releases(start_date=start_date, end_date=end_date,
                                            state=MagState.discovered, date_type=MagDateType.release,
                                            reverse=True)
            self.assertEqual(releases[0].row_key, '2020-05-01')
            self.assertEqual(releases[1].row_key, '2020-04-24')
            self.assertEqual(releases[2].row_key, '2020-04-17')

        finally:
            # Clean up
            service = TableService(account_name=self.account_name, account_key=self.account_key)
            for container in containers:
                service.delete_entity(MagRelease.TABLE_NAME, 'mag', container.name.replace("mag-", ""))
