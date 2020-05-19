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
from typing import List

import azure.functions as func

from mag_archiver.mag import MagArchiverClient, MagState, MagDateType, MagRelease, MagTask


def main(timer: func.TimerRequest) -> None:
    # Get environment variables
    account_name = os.getenv('STORAGE_ACCOUNT_NAME')
    account_key = os.getenv('STORAGE_ACCOUNT_KEY')
    target_container = os.getenv('TARGET_CONTAINER')
    assert account_name is not None and account_key is not None and target_container is not None, \
        "The environment variables STORAGE_ACCOUNT_NAME,  STORAGE_ACCOUNT_KEY and TARGET_CONTAINER must be set."

    # List MAG containers in storage account
    client = MagArchiverClient(account_name=account_name, account_key=account_key)
    containers = client.list_containers(last_modified_thresh=1)

    # Update MagReleases Table based on discovered containers
    client.update_releases(containers)

    # List all discovered MAG releases and sort from oldest to newest based on release date
    releases: List[MagRelease] = client.list_releases(state=MagState.discovered, date_type=MagDateType.release,
                                                      reverse=False)

    # If 1 or more MAG releases was found then process the oldest one
    if len(releases) >= 1:
        release = releases[0]
        # Copy source container to target container
        release.update_task(MagTask.copying_to_release_container)  # Set state to copying. TODO: should be changed later
        # when we actually archive the release
        target_folder = release.source_container
        release.archive(target_container, target_folder)
        release.update_state(MagState.archived)

        # Delete source container
        release.update_task(MagTask.deleting_source_container)
        release.cleanup()
        release.update_task(MagTask.done)
        release.update_state(MagState.done)
