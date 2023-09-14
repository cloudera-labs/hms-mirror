/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hadoop.hms.mirror;

public enum DistcpFlow {

    /*
    Data is being 'pulled' from the source cluster by the target cluster (RIGHT 'pulls' from LEFT).

    This is typical in sidecar on-prem migrations, where the newer cluster is usually where the 'distcp' commands
    are run.
     */
    PULL,
    /*
    Data is 'pushed' to the target from the 'source' cluster.  LEFT pushes to target.  This is the typical pattern
    for on-prem to cloud migrations since the cloud environments can not (usually) access the on-prem environments.
     */
    PUSH,
    /*
    Data is 'pushed' from LEFT to transition area.  Then the 'data' is PULL from the 'transition' area by the RIGHT.
     */
    PUSH_PULL
}
