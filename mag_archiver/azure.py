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

import time
from typing import List, Any

from azure.cosmosdb.table.tableservice import TableService
from azure.storage.blob import BlobServiceClient, ContainerProperties, ContainerClient, BlobProperties, BlobClient


def make_account_url(account_name: str) -> str:
    """ Make an Azure Storage account URL from an account name.

    :param account_name: Azure Storage account name.
    :return: the account URL.
    """

    return f'https://{account_name}.blob.core.windows.net'


def create_table(account_name: str, account_key: str, table_name: str) -> bool:
    """ Create an Azure Table.

    :param account_name: Azure Storage account name.
    :param account_key: Azure Storage account key.
    :param table_name: name of the table to create.
    :return: whether the table was created or not.
    """

    service = TableService(account_name=account_name, account_key=account_key)
    return service.create_table(table_name)


def delete_table(account_name: str, account_key: str, table_name: str):
    """ Delete an Azure Table.

    :param account_name: Azure Storage account name.
    :param account_key: Azure Storage account key.
    :param table_name: name of the table to delete.
    :return: whether the table was deleted or not.
    """

    service = TableService(account_name=account_name, account_key=account_key)
    return service.delete_table(table_name)


def create_container(account_name: str, account_key: str, container_name: str) -> ContainerClient:
    """ Create an Azure Storage Blob Container.

    :param account_name: Azure Storage account name.
    :param account_key: Azure Storage account key.
    :param container_name: name of the container to create.
    :return: the container client of the created container.
    """

    account_url = make_account_url(account_name)
    client: BlobServiceClient = BlobServiceClient(account_url, account_key)
    return client.create_container(container_name)


def delete_container(account_name: str, account_key: str, container_name: str) -> None:
    """ Delete an Azure Storage Blob Container.

    :param account_name: Azure Storage account name.
    :param account_key: Azure Storage account key.
    :param container_name: name of the container to delete.
    :return: None.
    """

    account_url = make_account_url(account_name)
    client: BlobServiceClient = BlobServiceClient(account_url, account_key)
    client.delete_container(container_name)


def list_containers(account_name: str, account_key: str) -> List[ContainerProperties]:
    """ List all containers in an Azure Storage account.

    :param account_name: Azure Storage account name.
    :param account_key: Azure Storage account key.
    :return: list of all containers found in the Azure Storage account.
    """

    account_url = make_account_url(account_name)
    client: BlobServiceClient = BlobServiceClient(account_url, account_key)
    containers = client.list_containers(include_metadata=True)
    return [c for c in containers]


def create_blob(account_name: str, account_key: str, container_name: str, blob_name: str, blob_data: Any) -> BlobClient:
    """ Create an Azure Storage blob.

    :param account_name: Azure Storage account name.
    :param account_key: Azure Storage account key.
    :param container_name: name of the container to create the blob on.
    :param blob_name: the name of the blob to create.
    :param blob_data: the data to create the blob from.
    :return: the blob client for the created blob.
    """

    account_url = make_account_url(account_name)
    client: BlobServiceClient = BlobServiceClient(account_url, account_key)
    blob_client: BlobClient = client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(blob_data)
    return blob_client


def list_blobs(account_name: str, account_key: str, container_name: str) -> List[BlobProperties]:
    """ List the blobs in an Azure Storage Blob Container.

    :param account_name: Azure Storage account name.
    :param account_key: Azure Storage account key.
    :param container_name: the name of the container to list the blobs in.
    :return: a list of the blobs that were found in the container.
    """

    account_url = make_account_url(account_name)
    client: ContainerClient = ContainerClient(account_url, container_name, credential=account_key)
    blobs = client.list_blobs()
    return [b for b in blobs]


def copy_container(account_name: str, account_key: str, source_container: str, target_container: str,
                   target_folder: str) -> None:
    """ Copy the blobs from one Azure Storage Blob Container to another.

    :param account_name: Azure Storage account name.
    :param account_key: Azure Storage account key.
    :param source_container: the source container.
    :param target_container: the target container.
    :param target_folder: the target folder.
    :return:
    """

    blobs = list_blobs(account_name, account_key, source_container)

    # Create copy jobs
    targets = []
    account_url = make_account_url(account_name)
    client: BlobServiceClient = BlobServiceClient(account_url, credential=account_key)

    for blob in blobs:
        source_path = f"{account_url}/{source_container}/{blob.name}"
        target_path = f"{target_folder}/{blob.name}"

        target_blob = client.get_blob_client(target_container, target_path)
        target_blob.start_copy_from_url(source_path)
        targets.append(target_blob)

    # Wait for copy jobs to finish
    # TODO: How do you know if it failed?
    while True:
        not_finished = []
        for blob in targets:
            p = blob.get_blob_properties()
            print(p.copy.status)

            if p.copy.status != 'success':
                not_finished.append(blob)
        targets = not_finished

        if len(targets) == 0:
            break
        else:
            time.sleep(5)
