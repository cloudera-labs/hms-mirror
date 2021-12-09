/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms.mirror.feature;

import com.cloudera.utils.hadoop.hms.mirror.EnvironmentTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;

public class BadParquetDefFeature extends BaseFeature implements Feature {
    private final String ROW_FORMAT_DELIMITED = "ROW FORMAT DELIMITED";
    private final String STORED_AS_INPUTFORMAT = "STORED AS INPUTFORMAT";
    private final String OUTPUTFORMAT = "OUTPUTFORMAT";
    private final String ROW_FORMAT_SERDE = "ROW FORMAT SERDE";
    private final String ROW_FORMAT_SERDE_CLASS = "'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'";
    private final String INPUT_FORMAT_CLASS = "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'";
    private final String OUTPUT_FORMAT_CLASS = "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'";
    private final String STORED_AS_PARQUET = "STORED AS PARQUET";
    private static Logger LOG = LogManager.getLogger(BadParquetDefFeature.class);

    public String getDescription() {
        return "Table schema definitions for Parquet files that don't include include INPUT and " +
                "OUTPUT format but no ROW FORMAT SERDE will not translate correctly.  This process will " +
                "remove the invalid declarations and set STORED AS PARQUET";
    }

    @Override
    public Boolean applicable(EnvironmentTable envTable) {
        return applicable(envTable.getDefinition());
    }

    @Override
    public Boolean applicable(List<String> schema) {
        Boolean rtn = Boolean.FALSE;

        int saiIdx = indexOf(schema, STORED_AS_INPUTFORMAT);
        if (saiIdx > 0) {
            // Find the "STORED AS INPUTFORMAT" index
            if (schema.get(saiIdx + 1).trim().equals(INPUT_FORMAT_CLASS)) {
                int of = indexOf(schema, OUTPUTFORMAT);
                if (of > saiIdx + 1) {
                    // Now check for OUTPUT class match
                    if (schema.get(of + 1).trim().equals(OUTPUT_FORMAT_CLASS)) {
                        rtn = Boolean.TRUE;
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
     STORED AS INPUTFORMAT
     'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
     OUTPUTFORMAT
     'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
     */
    public Boolean fixSchema(List<String> schema) {
        if (applicable(schema)) {
//            schema = addEscaped(schema);
            LOG.debug("Checking if table has bad PARQUET definition");
            // find the index of the ROW_FORMAT_DELIMITED

            int startRange = -1;
            int endRange = -1;
            int rfdIdx = indexOf(schema, ROW_FORMAT_DELIMITED);
            if (rfdIdx > 0) {
                startRange = rfdIdx;
            }

            if (startRange == -1) {
                int rfsIdx = indexOf(schema, ROW_FORMAT_SERDE);
                if (rfsIdx > 0) {
                    startRange = rfsIdx;
                }
            }

            if (startRange == -1) {
                int saiIdx = indexOf(schema, STORED_AS_INPUTFORMAT);
                if (saiIdx > 0) {
                    startRange = saiIdx;
                }
            }

            int of = indexOf(schema, OUTPUTFORMAT);
            if (of > 0) {
                endRange = of + 2;
            }

            if ((startRange < endRange) & (startRange > 0)) {
                LOG.debug("BAD PARQUET definition found. Correcting...");
                removeRange(startRange, endRange, schema);
                schema.add(startRange, STORED_AS_PARQUET);
            }
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

}
