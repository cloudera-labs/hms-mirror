/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.domain.support;

import lombok.Getter;

import static com.cloudera.utils.hms.mirror.domain.support.HiveVersion.*;

@Getter
public enum PlatformType {

    CDH5("CDH5", HIVE_1, Boolean.FALSE),
    CDH6("CDH6", HIVE_2, Boolean.FALSE),
    CDP7_0("CDP", HIVE_3, Boolean.FALSE),
    CDP7_1("CDP", HIVE_3, Boolean.FALSE),
    CDP7_1_9_SP1("CDP", HIVE_3, Boolean.TRUE),
    CDP7_2("CDP", HIVE_3, Boolean.TRUE),
    CDP7_3("CDP", HIVE_4, Boolean.TRUE),
    HDP2("HDP2", HIVE_1, Boolean.FALSE),
    HDP3("HDP3", HIVE_3, Boolean.FALSE),
    MAPR("MAPR", HIVE_1, Boolean.FALSE),
    EMR("EMR", HIVE_1, Boolean.FALSE),
    APACHE_HIVE1("APACHE_HIVE1", HIVE_1, Boolean.FALSE),
    APACHE_HIVE2("APACHE_HIVE2", HIVE_2, Boolean.FALSE),
    APACHE_HIVE3("APACHE_HIVE3", HIVE_3, Boolean.FALSE),
    APACHE_HIVE4("APACHE_HIVE4", HIVE_4, Boolean.TRUE);

    private final String platform;
    private final HiveVersion hiveVersion;
    private final boolean dbOwnerType;

    PlatformType(String platform, HiveVersion hiveVersion, boolean dbOwnerType) {
        this.platform = platform;
        this.hiveVersion = hiveVersion;
        this.dbOwnerType = dbOwnerType;
    }

}
