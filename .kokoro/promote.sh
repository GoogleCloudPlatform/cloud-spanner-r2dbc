#!/bin/bash
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Github action job to test core java library features on
# downstream client libraries before they are released.

set -eo pipefail

# STAGING_REPOSITORY_ID must be set
if [ -z "${STAGING_REPOSITORY_ID}" ]; then
  echo "Missing STAGING_REPOSITORY_ID environment variable"
  exit 1
fi

dir=$(dirname "$0")

source $dir/common.sh

pushd $dir/../

MAVEN_SETTINGS_FILE=$(realpath .)/settings.xml

setup_environment_secrets
create_settings_xml_file $MAVEN_SETTINGS_FILE

./mvnw nexus-staging:release -B \
  --settings=settings.xml \
  -DstagingRepositoryId=${STAGING_REPOSITORY_ID} \
  -P release

popd
