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

package com.cloudera.utils.hms.mirror.feature;

import com.cloudera.utils.hms.mirror.EnvironmentTable;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class BadOrcDefFeature extends BaseFeature implements Feature {
    private final String ROW_FORMAT_DELIMITED = "ROW FORMAT DELIMITED";
    private final String STORED_AS_INPUTFORMAT = "STORED AS INPUTFORMAT";
    private final String OUTPUTFORMAT = "OUTPUTFORMAT";
    private final String ROW_FORMAT_SERDE = "ROW FORMAT SERDE";
    private final String LAZY_SERDE = "'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'";
    private final String ORC_SERDE = "  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'";
    private final String INPUT_FORMAT_CLASS = "'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'";
    private final String OUTPUT_FORMAT_CLASS = "'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'";

    private final String STORED_AS_ORC = "STORED AS ORC";

    @Override
    public Boolean applicable(EnvironmentTable envTable) {
        return applicable(envTable.getDefinition());
    }

    @Override
    public Boolean applicable(List<String> schema) {
        Boolean rtn = Boolean.FALSE;

        int rfdIdx = indexOf(schema, ROW_FORMAT_DELIMITED);
        if (rfdIdx > 0) {
            // Find the "STORED AS INPUTFORMAT" index
            int saiIdx = indexOf(schema, STORED_AS_INPUTFORMAT);
            if (saiIdx > rfdIdx) {
                if (schema.get(saiIdx + 1).trim().equals(INPUT_FORMAT_CLASS)) {
                    int of = indexOf(schema, OUTPUTFORMAT);
                    if (of > saiIdx + 1) {
                        if (schema.get(of + 1).trim().equals(OUTPUT_FORMAT_CLASS)) {
                            rtn = Boolean.TRUE;
                        }
                    }
                }
            }
        }

        return rtn;
    }

    @Override
    public Boolean fixSchema(EnvironmentTable envTable) {
        return fixSchema(envTable.getDefinition());
    }

    @Override
    /**
     * ROW FORMAT DELIMITED
     *   FIELDS TERMINATED BY '\t'
     *   LINES TERMINATED BY '\n'
     * STORED AS INPUTFORMAT
     *   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     * OUTPUTFORMAT
     *   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     */
    public Boolean fixSchema(List<String> schema) {
        if (applicable(schema)) {
//            schema = addEscaped(schema);
            log.debug("Checking if table has bad ORC definition");
            // find the index of the ROW_FORMAT_DELIMITED
            int rfdIdx = indexOf(schema, ROW_FORMAT_DELIMITED);
            if (rfdIdx > 0) {
                // Find the "STORED AS INPUTFORMAT" index
                int saiIdx = indexOf(schema, STORED_AS_INPUTFORMAT);
                if (saiIdx > rfdIdx) {
                    if (schema.get(saiIdx + 1).trim().equals("'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'")) {
                        int of = indexOf(schema, OUTPUTFORMAT);
                        if (of > saiIdx + 1) {
                            if (schema.get(of + 1).trim().equals("'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'")) {
                                log.debug("BAD ORC definition found. Correcting...");
                                // All matches.  Need to replace with serde
                                removeRange(rfdIdx, of + 2, schema);
                                schema.add(rfdIdx, STORED_AS_ORC);
                            }
                        }
                    }
                }
            }
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public String getDescription() {
        return "Table schema definitions for ORC files that include ROW FORMAT DELIMITED " +
                "declarations are invalid.  This process will remove the invalid declarations " +
                "and set STORED AS ORC";
    }

}
