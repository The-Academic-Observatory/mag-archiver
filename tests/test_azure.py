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

from mag_archiver.azure import list_containers, copy_container, list_blobs


class TestMag(unittest.TestCase):
    account_name: str
    account_key: str

    def __init__(self, *args, **kwargs):
        super(TestMag, self).__init__(*args, **kwargs)

        self.account_name = os.getenv('STORAGE_ACCOUNT_NAME')
        self.account_key = os.getenv('STORAGE_ACCOUNT_KEY')

    def test_list_containers(self):
        containers = list_containers(self.account_name, self.account_key)
        self.assertEqual(len(containers), 41)

    def test_delete_container(self):
        pass

    def test_copy_container(self):
        source_container = 'mag-2020-04-24'
        target_container = 'mag-snapshots-dev'
        target_folder = 'mag-2020-04-24'
        copy_container(self.account_name, self.account_key, source_container, target_container, target_folder)

    def test_list_blobs_container(self):
        blobs = list_blobs(TestMag.account_name, TestMag.account_key, 'mag-2020-04-24')
        self.assertEqual(len(blobs), 30)
