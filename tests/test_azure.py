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
import uuid

from azure.storage.blob import ContainerClient, BlobClient

from mag_archiver.azure import list_containers, copy_container, list_blobs, create_container, delete_container, \
    create_blob, delete_table, create_table


def generate_names(num_names, prefix):
    # Create containers
    names = []
    for i in range(num_names):
        name = make_unique_name(prefix)
        names.append(name)
    return names


def make_unique_name(prefix: str):
    return f"{prefix}-{str(uuid.uuid4())}"


class TestAzure(unittest.TestCase):
    account_name: str
    account_key: str

    def __init__(self, *args, **kwargs):
        super(TestAzure, self).__init__(*args, **kwargs)
        self.account_name = os.getenv('TEST_AZURE_STORAGE_ACCOUNT_NAME')
        self.account_key = os.getenv('TEST_AZURE_STORAGE_ACCOUNT_KEY')

    def test_create_table(self):
        table_name = 'TestCreateTable'
        try:
            # Create a table
            result = create_table(self.account_name, self.account_key, table_name)
            self.assertTrue(result)
        finally:
            # Cleanup
            delete_table(self.account_name, self.account_key, table_name)

    def test_delete_table(self):
        table_name = 'TestDeleteTable'
        try:
            # Create a table
            create_table(self.account_name, self.account_key, table_name)

            # Delete table
            result = delete_table(self.account_name, self.account_key, table_name)
            self.assertTrue(result)
        finally:
            # Cleanup
            delete_table(self.account_name, self.account_key, table_name)

    def test_create_container(self):
        container_name = make_unique_name('test-create-container')
        try:
            # Create a container
            result: ContainerClient = create_container(self.account_name, self.account_key, container_name)
            self.assertIsInstance(result, ContainerClient)
            self.assertEqual(result.container_name, container_name)
        finally:
            # Cleanup
            delete_container(self.account_name, self.account_key, container_name)

    def test_create_blob(self):
        container_name = make_unique_name('test-create-blob')
        try:
            # Create a container for the blob
            create_container(self.account_name, self.account_key, container_name)

            # Create a blob
            blob_name = make_unique_name('test-create-blob') + '.txt'
            blob_data = 'Hello world!'
            result: BlobClient = create_blob(self.account_name, self.account_key, container_name, blob_name, blob_data)
            self.assertIsInstance(result, BlobClient)
            self.assertEqual(result.blob_name, blob_name)
        finally:
            # Cleanup
            delete_container(self.account_name, self.account_key, container_name)

    def test_list_containers(self):
        num_containers = 3
        names = generate_names(num_containers, 'test-list-containers')
        try:
            # Create containers
            for name in names:
                create_container(self.account_name, self.account_key, name)

            # List containers
            containers = list_containers(self.account_name, self.account_key)
            self.assertEqual(len(containers), num_containers + 1)
        finally:
            # Cleanup
            for name in names:
                delete_container(self.account_name, self.account_key, name)

    def test_delete_container(self):
        container_name = make_unique_name('test-delete-container')
        try:
            create_container(self.account_name, self.account_key, container_name)
            delete_container(self.account_name, self.account_key, container_name)
            containers = list_containers(self.account_name, self.account_key)
            container_names = [c.name for c in containers]
            self.assertNotIn(container_name, container_names)
        finally:
            # Cleanup
            delete_container(self.account_name, self.account_key, container_name)

    def test_list_blobs(self):
        container_name = make_unique_name('test-list-blobs')

        try:
            # Create container to store the blobs
            create_container(self.account_name, self.account_key, container_name)

            # Create the blobs
            num_blobs = 3
            blob_data = 'Hello world!'
            names = generate_names(num_blobs, 'test-list-blobs')
            for name in names:
                file_name = f'{name}.txt'
                create_blob(self.account_name, self.account_key, container_name, file_name, blob_data)

            # Check that we can find the blobs
            blobs = list_blobs(self.account_name, self.account_key, container_name)
            self.assertEqual(len(blobs), num_blobs)
        finally:
            # Cleanup
            delete_container(self.account_name, self.account_key, container_name)

    def test_copy_container(self):
        source_container = make_unique_name('test-copy-container-source')
        target_container = make_unique_name('test-copy-container-target')
        target_folder = 'target-folder'

        try:
            # Create containers and blobs
            create_container(self.account_name, self.account_key, source_container)
            create_container(self.account_name, self.account_key, target_container)

            # Create blobs in source container
            num_blobs = 3
            blob_data = 'Hello world!'
            names = generate_names(num_blobs, 'test-copy-container')
            for name in names:
                file_name = f'{name}.txt'
                create_blob(self.account_name, self.account_key, source_container, file_name, blob_data)

            # Copy blobs from one container to another
            copy_container(self.account_name, self.account_key, source_container, target_container, target_folder)

            # Check results
            blobs = list_blobs(self.account_name, self.account_key, target_container)
            self.assertEqual(len(blobs), num_blobs)
        finally:
            # Delete container
            delete_container(self.account_name, self.account_key, source_container)
            delete_container(self.account_name, self.account_key, target_container)
