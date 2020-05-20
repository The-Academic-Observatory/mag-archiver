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


def make_account_url(account_name) -> str:
    return f'https://{account_name}.blob.core.windows.net'


def create_table(account_name: str, account_key: str, table_name: str) -> bool:
    service = TableService(account_name=account_name, account_key=account_key)
    return service.create_table(table_name)


def delete_table(account_name: str, account_key: str, table_name: str):
    service = TableService(account_name=account_name, account_key=account_key)
    return service.delete_table(table_name)


def create_container(account_name: str, account_key: str, container_name: str) -> ContainerClient:
    account_url = make_account_url(account_name)
    client: BlobServiceClient = BlobServiceClient(account_url, account_key)
    return client.create_container(container_name)


def create_blob(account_name: str, account_key: str, container_name: str, blob_name: str, blob_data: Any) -> BlobClient:
    account_url = make_account_url(account_name)
    client: BlobServiceClient = BlobServiceClient(account_url, account_key)
    blob_client: BlobClient = client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(blob_data)
    return blob_client


def list_containers(account_name: str, account_key: str) -> List[ContainerProperties]:
    """ List all containers in the storage account

    :param account_name:
    :param account_key:
    :return:
    """

    account_url = make_account_url(account_name)
    client: BlobServiceClient = BlobServiceClient(account_url, account_key)
    containers = client.list_containers(include_metadata=True)
    return [c for c in containers]


def delete_container(account_name: str, account_key: str, container_name: str) -> None:
    """ Delete a blob container.

    :param account_name:
    :param account_key:
    :param container_name:
    :return:
    """

    account_url = make_account_url(account_name)
    client: BlobServiceClient = BlobServiceClient(account_url, account_key)
    client.delete_container(container_name)


def copy_container(account_name: str, account_key: str, source_container: str, target_container: str,
                   target_folder: str) -> None:
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


def list_blobs(account_name: str, account_key: str, container_name: str) -> List[BlobProperties]:
    account_url = make_account_url(account_name)
    client: ContainerClient = ContainerClient(account_url, container_name, credential=account_key)
    blobs = client.list_blobs()
    return [b for b in blobs]
