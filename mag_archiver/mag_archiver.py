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
import os
from typing import List

import azure.functions as func
import pendulum

from mag_archiver.mag import MagArchiverClient, MagState, MagDateType, MagRelease, MagTask


def main(timer: func.TimerRequest) -> None:
    logging.info('mag_archiver function: started')

    # Get environment variables
    account_name = os.getenv('STORAGE_ACCOUNT_NAME')
    account_key = os.getenv('STORAGE_ACCOUNT_KEY')
    target_container = os.getenv('TARGET_CONTAINER')

    # Check all environment variables are specified
    req_env_vars = account_name is not None and account_key is not None and target_container is not None
    req_env_vars_msg = "The environment variables STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY and TARGET_CONTAINER " \
                       "must be set."
    if not req_env_vars:
        logging.error(req_env_vars_msg)
    assert req_env_vars, req_env_vars_msg

    # List MAG containers in storage account
    client = MagArchiverClient(account_name=account_name, account_key=account_key)
    containers = client.list_containers(last_modified_thresh=1)
    logging.info(f'{client}')
    logging.info(f'Discovered containers: {containers}')

    # Update MagReleases Table based on discovered containers
    num_created, num_errors = client.update_releases(containers)
    logging.info(f'Update MagReleases table: num_created={num_created}, num_errors={num_errors}')

    # List all discovered MAG releases and sort from oldest to newest based on release date
    releases: List[MagRelease] = client.list_releases(state=MagState.discovered, date_type=MagDateType.release,
                                                      reverse=False)
    num_releases = len(releases)
    logging.info(f'Number of discovered MAG releases: {num_releases}')
    logging.info(f'Discovered MAG releases: {releases}')

    # If 1 or more MAG releases was found then process the oldest one
    if len(releases) >= 1:
        release: MagRelease = releases[0]
        # We want to have the same directory structure here, as this is how the data will be structured in the
        # Google Cloud Storage bucket when the data is transferred with the Google Cloud data transfer service
        target_folder = f'telescopes/mag/{release.source_container}'
        logging.info(f'Processing: {release}')

        # Update task to copying
        release.task = MagTask.copying_to_release_container
        logging.info(f'Update task to copying: {release}')
        release.update()
        logging.info(f'Update task to copying finished: {release}')

        # Copy source container to target container
        logging.info(f'Archive: {release}')
        release.archive(target_container, target_folder)  # TODO: how do you know if the copying failed?
        logging.info(f'Archive finished: {release}')

        # Update state to archived
        #   - set state to archived
        #   - add release container and release path
        #   - set archived date to now
        #   - set task to cleaning up
        release.state = MagState.archived
        release.release_container = target_container
        release.release_path = target_folder
        release.archived_date = pendulum.utcnow()
        release.task = MagTask.cleaning_up
        logging.info(f'Update state to archived: {release}')
        release.update()
        logging.info(f'Update state to archived finished: {release}')

        # Cleanup: delete source container
        logging.info(f'Cleanup: {release}')
        release.cleanup()
        logging.info(f'Cleanup finished: {release}')

        # Update state to done
        # - set state and task to done
        # - set done date to now
        release.state = MagState.done
        release.task = MagTask.done
        release.done_date = pendulum.utcnow()
        logging.info(f'Update state to done: {release}')
        release.update()
        logging.info(f'Update state to done finished: {release}')
    else:
        logging.info(f'No discovered releases to process')

    logging.info('mag_archiver function: finished')
