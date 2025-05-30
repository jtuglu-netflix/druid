#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
set -x

export DRUID_OPERATOR_VERSION=v1.0.0
export KUBECTL="/usr/local/bin/kubectl"

# Prepare For Druid-Operator
rm -rf druid-operator
git clone https://github.com/datainfrahq/druid-operator.git
cd druid-operator
git checkout -b druid-operator-$DRUID_OPERATOR_VERSION $DRUID_OPERATOR_VERSION
cd ..

# Deploy Druid Operator and Druid CR spec
$KUBECTL create -f druid-operator/deploy/service_account.yaml
$KUBECTL create -f druid-operator/deploy/role.yaml
$KUBECTL create -f druid-operator/deploy/role_binding.yaml
$KUBECTL create -f druid-operator/deploy/crds/druid.apache.org_druids.yaml
$KUBECTL create -f druid-operator/deploy/operator.yaml

echo "Setup Druid Operator on K8S Done!"
