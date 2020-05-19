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

import logging
import re
from enum import Enum
from typing import Optional, List

import pendulum
from azure.common import AzureConflictHttpError
from azure.cosmosdb.table.models import EntityProperty, EdmType
from azure.cosmosdb.table.tableservice import TableService
from azure.storage.blob import ContainerProperties
from pendulum import Pendulum

from mag_archiver.azure import copy_container, delete_container
from mag_archiver.azure import list_containers


class MagState(Enum):
    discovered = 'discovered'
    archived = 'archived'
    done = 'done'


class MagTask(Enum):
    not_started = 'not-started'
    copying_to_vm = 'copying-to-vm'
    archiving = 'archiving'
    copying_to_release_container = 'copying-to-release-container'
    deleting_source_container = 'deleting-source-container'
    done = 'done'


class MagDateType(Enum):
    release = 'ReleaseDate'
    discovered = 'DiscoveredDate'
    archived = 'ArchivedDate'
    done = 'DoneDate'

    @classmethod
    def map(cls):
        return {
            MagDateType.release: 'release_date',
            MagDateType.discovered: 'discovered_date',
            MagDateType.archived: 'archived_date',
            MagDateType.done: 'done_date',
        }

    def attr(self):
        map_ = MagDateType.map()
        return map_[self]


class MagContainer:

    def __init__(self, name: str, last_modified: Pendulum, release_date: Pendulum):
        self.name = name
        self.last_modified = last_modified
        self.release_date = release_date


class MagRelease:
    TABLE_NAME = 'MagReleases'
    __PARTITION_KEY = 'PartitionKey'
    __ROW_KEY = 'RowKey'
    __STATE = 'State'
    __TASK = 'Task'
    __RELEASE_DATE = 'ReleaseDate'
    __SOURCE_CONTAINER = 'SourceContainer'
    __SOURCE_CONTAINER_LAST_MODIFIED = 'SourceContainerLastModified'
    __RELEASE_CONTAINER = 'ReleaseContainer'
    __RELEASE_PATH = 'ReleasePath'
    __DISCOVERED_DATE = 'DiscoveredDate'
    __ARCHIVED_DATE = 'ArchivedDate'
    __DONE_DATE = 'DoneDate'

    def __init__(self, partition_key: str, row_key: str, state: MagState, task: MagTask, release_date: Pendulum,
                 source_container: str, source_container_last_modified: Pendulum, release_container: str,
                 release_path: str, discovered_date: Pendulum, archived_date: Pendulum, done_date: Pendulum,
                 account_name: Optional[str] = None, account_key: Optional[str] = None):
        """
        :param partition_key: the partition key.
        :param row_key: the row key.
        :param state: the state that the MAG release is in.
        :param task: the current task that is being executed.
        :param release_date: the official release date for the MAG release. Note that releases are published much later
        than the official date for the release.
        :param source_container: the container where the MAG release was provisioned on.
        :param source_container_last_modified: the date that the source container was last modified.
        :param release_container: the container where the MAG releases are staged for data transfer.
        :param release_path: the path to the MAG release on the release container.
        :param discovered_date: the date that the MAG release was discovered.
        :param archived_date: the date the the MAG release was archived. Set to the minimum date, i.e.
        1601-01-01T00:00:00.000Z if not specified.
        :param done_date: the date the MAG release is ready. Set to the minimum date, i.e.
        1601-01-01T00:00:00.000Z if not specified.
        """

        self.partition_key = partition_key
        self.row_key = row_key
        self.state = state
        self.task = task
        self.release_date = release_date
        self.source_container = source_container
        self.source_container_last_modified = source_container_last_modified
        self.release_container = release_container
        self.release_path = release_path
        self.discovered_date = discovered_date
        self.archived_date = archived_date
        self.done_date = done_date
        self.account_name = account_name
        self.account_key = account_key

    def __assert_account(self, msg):
        assert self.account_name is not None and self.account_key is not None, msg

    def __table_service(self):
        return TableService(account_name=self.account_name, account_key=self.account_key)

    def create(self):
        self.__assert_account("MagRelease.create: account_name and account_key must be supplied.")
        service = self.__table_service()
        success = True
        try:
            service.insert_entity(MagRelease.TABLE_NAME, self.to_entity())
        except AzureConflictHttpError as e:
            success = False
            logging.error(e)

        return success

    def archive(self, target_container: str, target_folder: str):
        self.__assert_account("MagRelease.archive: account_name and account_key must be supplied.")
        return copy_container(self.account_name, self.account_key, self.source_container, target_container,
                              target_folder)

    def cleanup(self):
        self.__assert_account("MagRelease.cleanup: account_name and account_key must be supplied.")
        return delete_container(self.account_name, self.account_key, self.source_container)

    def update_state(self, state: MagState):
        assert state is not MagState.discovered, "MagRelease.update_state: only accepts archived and done states."
        self.__assert_account("MagRelease.update_state: account_name and account_key must be supplied.")

        # Create entity
        entity = dict()
        entity[MagRelease.__PARTITION_KEY] = self.partition_key
        entity[MagRelease.__ROW_KEY] = self.row_key
        entity[MagRelease.__STATE] = state.value
        current_dt = pendulum.datetime.now(pendulum.timezone('UTC'))
        if state is MagState.archived:
            entity[MagRelease.__ARCHIVED_DATE] = current_dt
        elif state is MagState.done:
            entity[MagRelease.__DONE_DATE] = current_dt

        # Update properties
        service = self.__table_service()
        return service.merge_entity(MagRelease.TABLE_NAME, entity)

    def update_task(self, task: MagTask):
        self.__assert_account("MagRelease.update_task: account_name and account_key must be supplied.")

        # Create entity
        entity = dict()
        entity[MagRelease.__PARTITION_KEY] = self.partition_key
        entity[MagRelease.__ROW_KEY] = self.row_key
        entity[MagRelease.__TASK] = task.value

        # Update properties
        service = self.__table_service()
        return service.merge_entity(MagRelease.TABLE_NAME, entity)

    @staticmethod
    def from_entity(entity: dict, account_name: Optional[str] = None, account_key: Optional[str] = None):
        partition_key_ = entity[MagRelease.__PARTITION_KEY]
        row_key_ = entity[MagRelease.__ROW_KEY]
        state_ = MagState(entity[MagRelease.__STATE])
        task_ = MagTask(entity[MagRelease.__TASK])
        release_date_ = pendulum.instance(entity[MagRelease.__RELEASE_DATE])
        source_container_ = entity[MagRelease.__SOURCE_CONTAINER]
        source_container_last_modified_ = entity[MagRelease.__SOURCE_CONTAINER_LAST_MODIFIED]
        release_container_ = entity[MagRelease.__RELEASE_CONTAINER]
        release_path_ = entity[MagRelease.__RELEASE_PATH]
        discovered_date_ = pendulum.instance(entity[MagRelease.__DISCOVERED_DATE])
        archived_date_ = pendulum.instance(entity[MagRelease.__ARCHIVED_DATE])
        done_date_ = pendulum.instance(entity[MagRelease.__DONE_DATE])

        return MagRelease(partition_key_, row_key_, state_, task_, release_date_, source_container_,
                          source_container_last_modified_, release_container_, release_path_, discovered_date_,
                          archived_date_, done_date_, account_name=account_name, account_key=account_key)

    def to_entity(self) -> dict:
        entity = dict()
        entity[MagRelease.__PARTITION_KEY] = self.partition_key
        entity[MagRelease.__ROW_KEY] = self.row_key
        entity[MagRelease.__STATE] = self.state.value
        entity[MagRelease.__TASK] = self.task.value
        entity[MagRelease.__RELEASE_DATE] = EntityProperty(EdmType.DATETIME, value=self.release_date)
        entity[MagRelease.__SOURCE_CONTAINER] = self.source_container
        entity[MagRelease.__SOURCE_CONTAINER_LAST_MODIFIED] = \
            EntityProperty(EdmType.DATETIME, value=self.source_container_last_modified)
        entity[MagRelease.__RELEASE_CONTAINER] = self.release_container
        entity[MagRelease.__RELEASE_PATH] = self.release_path
        entity[MagRelease.__DISCOVERED_DATE] = EntityProperty(EdmType.DATETIME, value=self.discovered_date)
        entity[MagRelease.__ARCHIVED_DATE] = EntityProperty(EdmType.DATETIME, value=self.archived_date)
        entity[MagRelease.__DONE_DATE] = EntityProperty(EdmType.DATETIME, value=self.done_date)
        return entity

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"MAG release {self.release_date.strftime('%Y-%m-%d')}"


def make_mag_query(start_date: Optional[Pendulum] = None, end_date: Optional[Pendulum] = None,
                   state: Optional[MagState] = None, date_type: MagDateType = MagDateType.release):
    """ Make a query for querying the Microsoft Academic releases.

    :param start_date: start date for the query.
    :param end_date: end date for the query.
    :param state: the state of the MAG release.
    :param date_type: the date type to query.
    :return:
    """

    commands = []
    date_format = '%Y-%m-%dT%H:%MZ'

    if state is not None:
        commands.append(f"State eq '{state.value}'")

    if start_date is not None:
        commands.append(f"{date_type.value} ge datetime'{start_date.strftime(date_format)}'")

    if end_date is not None:
        commands.append(f"{date_type.value} lt datetime'{end_date.strftime(date_format)}'")

    query = ' and '.join(commands)
    return query


class MagArchiverClient:
    __MAG_RELEASE_RE = re.compile("mag-[0-9]{4}-[0-9]{2}-[0-9]{2}")

    def __init__(self, account_name: Optional[str] = None, account_key: Optional[str] = None,
                 sas_token: Optional[str] = None):
        """

        :param account_name: the name of the Azure storage account where the Microsoft Academic Graph releases are
        provisioned and the `MagReleases` Azure Storage Table is stored.
        :param account_key: the key to the storage account.
        :param sas_token: the shared access token to use for authentication instead of the account_key.
        """

        self.account_name = account_name
        self.account_key = account_key
        self.sas_token = sas_token

    def list_containers(self, last_modified_thresh: Optional[float] = None, reverse: bool = False) \
            -> List[MagContainer]:
        """ List all blob containers holding MAG releases.

        :param last_modified_thresh: only include containers that were last modified greater than a specific number of
        hours.
        :param reverse: sort from oldest to newest using release_date datetime.
        :return:
        """

        # List all containers in the storage account
        containers: List[ContainerProperties] = list_containers(self.account_name, self.account_key)

        tz = pendulum.timezone('UTC')
        current_time = pendulum.datetime.now(tz)

        # Select all containers containing MAG releases that were last updated greater than
        mag_containers = []
        for container in containers:
            container_name = container.name
            last_modified = pendulum.instance(container.last_modified, tz=tz)
            hours_since_modified = current_time.diff(last_modified).in_hours()

            # Only include containers with a name that matches the MAG container pattern
            if MagArchiverClient.__MAG_RELEASE_RE.match(container_name) is not None:
                # Add of no last_modified_thresh supplied or if the hours since modified is greater than the last
                # modified threshold
                if last_modified_thresh is None or last_modified_thresh < hours_since_modified:
                    release_date_str = container.name.replace("mag-", "")
                    release_date = pendulum.parse(release_date_str)
                    mag_container = MagContainer(container_name, last_modified, release_date)
                    mag_containers.append(mag_container)

        # Sort from oldest to newest
        mag_containers.sort(key=lambda c: c.release_date, reverse=reverse)
        return mag_containers

    def update_releases(self, containers: List[MagContainer]):
        min_date = pendulum.datetime(1601, 1, 1)
        discovered_date = pendulum.now()

        # Get all entities
        table_service = TableService(account_name=self.account_name, account_key=self.account_key)
        entities = table_service.query_entities(MagRelease.TABLE_NAME)

        # Get all containers that are not in the MagReleases table
        new_container_index = dict.fromkeys(set([container.name for container in containers]) -
                                            set([entity['SourceContainer'] for entity in entities]), 0)
        num_new_containers = len(new_container_index)
        logging.info(f"Num new containers discovered: {num_new_containers}")

        # Only add new containers
        num_updated = 0
        num_errors = 0
        for container in containers:
            if container.name in new_container_index:
                partition_key = 'mag'
                row_key = container.release_date.strftime("%Y-%m-%d")
                release = MagRelease(partition_key, row_key, MagState.discovered, MagTask.not_started,
                                     container.release_date, container.name, container.last_modified, '', '',
                                     discovered_date, min_date, min_date, account_name=self.account_name,
                                     account_key=self.account_key)
                success = release.create()
                if success:
                    num_updated += 1
                else:
                    num_errors += 1
        return num_updated, num_errors

    def list_releases(self, start_date: Optional[Pendulum] = None,
                      end_date: Optional[Pendulum] = None, state: Optional[MagState] = None,
                      date_type: MagDateType = MagDateType.release, reverse: bool = False):
        """ List Microsoft Academic releases.

        :param start_date: start date for the query.
        :param end_date: end date for the query.
        :param state: the state of the MAG release.
        :param date_type: the date type to query.
        :param reverse: whether to reverse sort the results. Sorting is performed on the date type.
        :return:
        """

        # Query and fetch releases
        table_service = TableService(account_name=self.account_name, account_key=self.account_key,
                                     sas_token=self.sas_token)
        query = make_mag_query(start_date=start_date, end_date=end_date, state=state)
        entities = table_service.query_entities('MagReleases', filter=query)

        # Convert entities into MagRelease objects
        releases = []
        for entity in entities:
            release = MagRelease.from_entity(entity, account_name=self.account_name, account_key=self.account_key)
            releases.append(release)

        # Sort from oldest to newest, unless reverse is set
        releases.sort(key=lambda r: getattr(r, MagDateType.attr(date_type)), reverse=reverse)
        return releases
