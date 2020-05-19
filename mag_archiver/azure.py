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
from typing import List

from azure.storage.blob import BlobServiceClient, ContainerProperties, ContainerClient, BlobProperties


def make_account_url(account_name) -> str:
    return f'https://{account_name}.blob.core.windows.net'


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


def delete_container(account_name: str, account_key: str, container_name: str):
    """ Delete a blob container.

    :param account_name:
    :param account_key:
    :param container_name:
    :return:
    """

    client: BlobServiceClient = BlobServiceClient(account_name, account_key)
    client.delete_container(container_name)


def copy_container(account_name: str, account_key: str, source_container: str, target_container: str,
                   target_folder: str):
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
