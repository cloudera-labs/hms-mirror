/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.domain;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PartitionDiscovery implements Cloneable {

    /*
    Partition Discovery is NOT enable by default in most cluster.  On the Metastore Leader, the `PartitionManagementTask`
    will run when
     */
    private boolean auto = Boolean.TRUE;
    /*
    Setting this will trigger an immediate msck on the table, which will affect performance of this job.  Consider
    using `auto`, to set the 'discovery'.  Make sure you activate and size the PartitionManagementTask process.
     */
    private boolean initMSCK = Boolean.TRUE;

    @Override
    public PartitionDiscovery clone() {
        try {
            PartitionDiscovery clone = (PartitionDiscovery) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
