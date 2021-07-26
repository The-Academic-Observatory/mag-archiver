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
from typing import Any, List, Optional

import pendulum

from azure.common import AzureConflictHttpError
from azure.cosmosdb.table.models import EdmType, EntityProperty
from azure.cosmosdb.table.tableservice import TableService
from azure.storage.blob import ContainerProperties
from mag_archiver.azure import copy_container, delete_container, list_containers


class MagState(Enum):
    """The state of the MAG release"""

    discovered = "discovered"
    archived = "archived"
    done = "done"


class MagTask(Enum):
    """The current task being executed"""

    not_started = "not-started"
    copying_to_vm = "copying-to-vm"
    archiving = "archiving"
    copying_to_release_container = "copying-to-release-container"
    cleaning_up = "cleaning-up"
    done = "done"


class MagDateType(Enum):
    """The date field type"""

    release = "ReleaseDate"
    discovered = "DiscoveredDate"
    archived = "ArchivedDate"
    done = "DoneDate"

    @classmethod
    def map(cls):
        return {
            MagDateType.release: "release_date",
            MagDateType.discovered: "discovered_date",
            MagDateType.archived: "archived_date",
            MagDateType.done: "done_date",
        }

    def attr(self):
        map_ = MagDateType.map()
        return map_[self]


def make_mag_query(
    start_date: Optional[pendulum.Date] = None,
    end_date: Optional[pendulum.Date] = None,
    state: Optional[MagState] = None,
    date_type: MagDateType = MagDateType.release,
) -> str:
    """Build a query for the MagReleases table.

    :param start_date: start date for the query (inclusive).
    :param end_date: end date for the query (exclusive).
    :param state: the state of the MAG release.
    :param date_type: the date type to query.
    :return: the query.
    """

    commands = []
    date_format = "%Y-%m-%dT%H:%MZ"

    if state is not None:
        commands.append(f"State eq '{state.value}'")

    if start_date is not None:
        commands.append(f"{date_type.value} ge datetime'{start_date.strftime(date_format)}'")

    if end_date is not None:
        commands.append(f"{date_type.value} lt datetime'{end_date.strftime(date_format)}'")

    query = " and ".join(commands)
    return query


def hide_if_not_none(secret: Any):
    """Hide a secret unless it is None, which means that the user didn't set it.

    :param secret: the secret.
    :return: None or 'hidden'
    """

    value = None
    if secret is not None:
        value = "hidden"
    return value


class MagContainer:
    def __init__(self, name: str, last_modified: pendulum.DateTime, release_date: pendulum.Date):
        """Microsoft Academic Graph container class.

        :param name: name of the container.
        :param last_modified: date that the container was last modified.
        :param release_date: date that the MAG release was released.
        """

        self.name = name
        self.last_modified = last_modified
        self.release_date = release_date

    def __str__(self):
        return f"Mag Container {self.name}"

    def __repr__(self):
        return (
            f'MagContainer({self.name}, {self.last_modified.strftime("%Y-%m-%d")}, '
            f'{self.release_date.strftime("%Y-%m-%d")})'
        )


class MagRelease:
    TABLE_NAME = "MagReleases"
    __PARTITION_KEY = "PartitionKey"
    __ROW_KEY = "RowKey"
    __STATE = "State"
    __TASK = "Task"
    __RELEASE_DATE = "ReleaseDate"
    __SOURCE_CONTAINER = "SourceContainer"
    __SOURCE_CONTAINER_LAST_MODIFIED = "SourceContainerLastModified"
    __RELEASE_CONTAINER = "ReleaseContainer"
    __RELEASE_PATH = "ReleasePath"
    __DISCOVERED_DATE = "DiscoveredDate"
    __ARCHIVED_DATE = "ArchivedDate"
    __DONE_DATE = "DoneDate"

    def __init__(
        self,
        partition_key: str,
        row_key: str,
        state: MagState,
        task: MagTask,
        release_date: pendulum.Date,
        source_container: str,
        source_container_last_modified: pendulum.DateTime,
        release_container: str,
        release_path: str,
        discovered_date: pendulum.DateTime,
        archived_date: pendulum.DateTime,
        done_date: pendulum.DateTime,
        account_name: Optional[str] = None,
        account_key: Optional[str] = None,
    ):
        """Microsoft Academic Graph release class.

        :param partition_key: partition key.
        :param row_key: the row key. partition_key + row_key form the primary key.
        :param state: state that the MAG release is in.
        :param task: current task that is being executed.
        :param release_date: the official release date for the MAG release. Note that releases are provisioned onto
        the Azure storage account about a week after the "release_date".
        :param source_container: container where the MAG release was provisioned to.
        :param source_container_last_modified: date that the source container was last modified.
        :param release_container: container where the MAG releases are staged for data transfer.
        :param release_path: path to the MAG release on the release container.
        :param discovered_date: date that the MAG release was discovered by the MAG archiver.
        :param archived_date: date the the MAG release was archived. Set to the minimum date, i.e.
        1601-01-01T00:00:00.000Z if not specified.
        :param done_date: date the MAG release is ready to be transferred. Set to the minimum date, i.e.
        1601-01-01T00:00:00.000Z if not specified.
        :param account_name: Azure Storage account name.
        :param account_key: Azure Storage account key.
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

    def create(self) -> bool:
        """Create the MagRelease instance in the MagReleases table.

        :return: whether the instance was created or not.
        """

        self.__assert_account("MagRelease.create: account_name and account_key must be supplied.")
        service = self.__table_service()
        success = True
        try:
            service.insert_entity(MagRelease.TABLE_NAME, self.to_entity())
        except AzureConflictHttpError as e:
            success = False
            logging.error(e)

        return success

    def delete(self) -> None:
        """Delete this MagRelease instance from the MagReleases table.

        :return: None.
        """

        self.__assert_account("MagRelease.delete: account_name and account_key must be supplied.")
        service = self.__table_service()
        service.delete_entity(MagRelease.TABLE_NAME, self.partition_key, self.row_key)

    def archive(self, target_container: str, target_folder: str):
        """Archive this MAG release into to the target container.

        TODO: in the future this method should compress the release before copying it.

        :param target_container: the container to copy the MAG release too.
        :param target_folder: the folder within the target container to copy the MAG release into.
        :return: None.
        """

        self.__assert_account("MagRelease.archive: account_name and account_key must be supplied.")
        return copy_container(
            self.account_name, self.account_key, self.source_container, target_container, target_folder
        )

    def cleanup(self):
        """Cleanup the source container.

        :return:
        """

        self.__assert_account("MagRelease.cleanup: account_name and account_key must be supplied.")
        return delete_container(self.account_name, self.account_key, self.source_container)

    def update(self):
        """Update this MAG release instance in the MagReleases table.

        :return:
        """

        self.__assert_account("MagRelease.update_state: account_name and account_key must be supplied.")

        # Update properties
        service = self.__table_service()
        return service.update_entity(MagRelease.TABLE_NAME, self.to_entity())

    @staticmethod
    def from_entity(entity: dict, account_name: Optional[str] = None, account_key: Optional[str] = None):
        """Create a MagRelease instance from an entity dictionary.

        :param entity: the entity returned from the Azure Table Storage API.
        :param account_name: Azure Storage account name. Optional, only required if the create, delete, archive,
        cleanup or update functions need to be used.
        :param account_key: Azure Storage account key. Optional, only required if the create, delete, archive,
        cleanup or update functions need to be used.
        :return: the MagRelease instance.
        """

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

        return MagRelease(
            partition_key_,
            row_key_,
            state_,
            task_,
            release_date_,
            source_container_,
            source_container_last_modified_,
            release_container_,
            release_path_,
            discovered_date_,
            archived_date_,
            done_date_,
            account_name=account_name,
            account_key=account_key,
        )

    def to_entity(self) -> dict:
        """Convert a MagRelease instance into an Azure Table Storage entity.

        :return: an Azure Table Storage entity.
        """

        entity = dict()
        entity[MagRelease.__PARTITION_KEY] = self.partition_key
        entity[MagRelease.__ROW_KEY] = self.row_key
        entity[MagRelease.__STATE] = self.state.value
        entity[MagRelease.__TASK] = self.task.value
        entity[MagRelease.__RELEASE_DATE] = EntityProperty(EdmType.DATETIME, value=self.release_date)
        entity[MagRelease.__SOURCE_CONTAINER] = self.source_container
        entity[MagRelease.__SOURCE_CONTAINER_LAST_MODIFIED] = EntityProperty(
            EdmType.DATETIME, value=self.source_container_last_modified
        )
        entity[MagRelease.__RELEASE_CONTAINER] = self.release_container
        entity[MagRelease.__RELEASE_PATH] = self.release_path
        entity[MagRelease.__DISCOVERED_DATE] = EntityProperty(EdmType.DATETIME, value=self.discovered_date)
        entity[MagRelease.__ARCHIVED_DATE] = EntityProperty(EdmType.DATETIME, value=self.archived_date)
        entity[MagRelease.__DONE_DATE] = EntityProperty(EdmType.DATETIME, value=self.done_date)
        return entity

    def __str__(self):
        return f'MagRelease {self.release_date.strftime("%Y-%m-%d")}'

    def __repr__(self):
        dt_format = "%Y-%m-%dT%H:%MZ"

        return (
            "MagRelease("
            f"{self.partition_key}, "
            f"{self.row_key}, "
            f"{self.state.value}, "
            f"{self.task.value}, "
            f"{self.release_date.strftime(dt_format)}, "
            f"{self.source_container}, "
            f"{self.source_container_last_modified.strftime(dt_format)}, "
            f"{self.release_container}, "
            f"{self.release_path}, "
            f"{self.discovered_date.strftime(dt_format)}, "
            f"{self.archived_date.strftime(dt_format)}, "
            f"{self.done_date.strftime(dt_format)}, "
            f"account_name={self.account_name}, "
            f"account_key={hide_if_not_none(self.account_key)})"
        )


class MagArchiverClient:
    __MAG_RELEASE_RE = re.compile("mag-[0-9]{4}-[0-9]{2}-[0-9]{2}")

    def __init__(
        self, account_name: Optional[str] = None, account_key: Optional[str] = None, sas_token: Optional[str] = None
    ):
        """

        :param account_name: the name of the Azure Storage account where the Microsoft Academic Graph releases are
        provisioned and the `MagReleases` Azure Storage Table is stored.
        :param account_key: the key to Azure Storage account.
        :param sas_token: the shared access token to use for authentication instead of the account_key.
        """

        self.account_name = account_name
        self.account_key = account_key
        self.sas_token = sas_token

    def list_containers(
        self, last_modified_thresh: Optional[float] = None, reverse: bool = False
    ) -> List[MagContainer]:
        """List all Azure Storage Blob Containers holding MAG releases.

        :param last_modified_thresh: only include containers that were last modified greater than or equal to a
        specific number of hours.
        :param reverse: sort from oldest to newest using release_date datetime.
        :return: the containers holding MAG releases.
        """

        # List all containers in the storage account
        containers: List[ContainerProperties] = list_containers(self.account_name, self.account_key)

        tz = pendulum.timezone("UTC")
        current_time = pendulum.now(tz)

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
                if last_modified_thresh is None or last_modified_thresh <= hours_since_modified:
                    release_date_str = container.name.replace("mag-", "")
                    release_date = pendulum.parse(release_date_str)
                    mag_container = MagContainer(container_name, last_modified, release_date)
                    mag_containers.append(mag_container)

        # Sort from oldest to newest
        mag_containers.sort(key=lambda c: c.release_date, reverse=reverse)
        return mag_containers

    def update_releases(self, containers: List[MagContainer]):
        """Update the releases in the MagReleases table based on a list of containers containing MAG releases.

        :param containers: the containers containing MAG releases.
        :return: the number of releases created and the number of errors.
        """

        min_date = pendulum.datetime(1601, 1, 1)
        discovered_date = pendulum.now()

        # Get all entities
        table_service = TableService(account_name=self.account_name, account_key=self.account_key)
        entities = table_service.query_entities(MagRelease.TABLE_NAME)

        # Get all containers that are not in the MagReleases table
        new_container_index = dict.fromkeys(
            set([container.name for container in containers]) - set([entity["SourceContainer"] for entity in entities]),
            0,
        )
        num_new_containers = len(new_container_index)
        logging.info(f"Num new containers discovered: {num_new_containers}")

        # Only add new containers
        num_created = 0
        num_errors = 0
        for container in containers:
            if container.name in new_container_index:
                partition_key = "mag"
                row_key = container.release_date.strftime("%Y-%m-%d")
                release = MagRelease(
                    partition_key,
                    row_key,
                    MagState.discovered,
                    MagTask.not_started,
                    container.release_date,
                    container.name,
                    container.last_modified,
                    "",
                    "",
                    discovered_date,
                    min_date,
                    min_date,
                    account_name=self.account_name,
                    account_key=self.account_key,
                )
                success = release.create()
                if success:
                    num_created += 1
                else:
                    num_errors += 1
        return num_created, num_errors

    def list_releases(
        self,
        start_date: Optional[pendulum.DateTime] = None,
        end_date: Optional[pendulum.DateTime] = None,
        state: Optional[MagState] = None,
        date_type: MagDateType = MagDateType.release,
        reverse: bool = False,
    ) -> List[MagRelease]:
        """List Microsoft Academic releases in the MagReleases Azure Storage Table.

        :param start_date: start date for the query.
        :param end_date: end date for the query.
        :param state: the state of the MAG release.
        :param date_type: the date type to query.
        :param reverse: whether to reverse sort the results. Sorting is performed on the date type.
        :return: the list of MagReleases found.
        """

        # Query and fetch releases
        table_service = TableService(
            account_name=self.account_name, account_key=self.account_key, sas_token=self.sas_token
        )
        query = make_mag_query(start_date=start_date, end_date=end_date, state=state, date_type=date_type)
        entities = table_service.query_entities("MagReleases", filter=query)

        # Convert entities into MagRelease objects
        releases = []
        for entity in entities:
            release = MagRelease.from_entity(entity, account_name=self.account_name, account_key=self.account_key)
            releases.append(release)

        # Sort from oldest to newest, unless reverse is set
        releases.sort(key=lambda r: getattr(r, MagDateType.attr(date_type)), reverse=reverse)
        return releases

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return (
            "MagArchiverClient("
            f"account_name={self.account_name}, "
            f"account_key={hide_if_not_none(self.account_key)}, "
            f"sas_token={hide_if_not_none(self.sas_token)})"
        )
