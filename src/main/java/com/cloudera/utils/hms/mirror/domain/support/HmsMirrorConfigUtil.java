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

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

public class HmsMirrorConfigUtil {

    public static String getResolvedDB(String database, HmsMirrorConfig config) {
        String rtn = null;
        // Set Local Value for adjustments
        String lclDb = database;
        // When dbp, set new value
        lclDb = (!isBlank(config.getDbPrefix()) ? config.getDbPrefix() + lclDb : lclDb);
        // Rename overrides prefix, otherwise use lclDb as its been set.
        rtn = (!isBlank(config.getDbRename()) ? config.getDbRename() : lclDb);
        return rtn;
    }


    public static boolean possibleConversions(HmsMirrorConfig config) {
        boolean conversions = Boolean.FALSE;
        if (config.getCluster(Environment.LEFT).isLegacyHive() &&
                (nonNull(config.getCluster(Environment.RIGHT)) && !config.getCluster(Environment.RIGHT).isLegacyHive())) {
            conversions = Boolean.TRUE;
        }

        return conversions;
    }
}
