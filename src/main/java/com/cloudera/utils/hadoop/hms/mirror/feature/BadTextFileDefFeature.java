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

package com.cloudera.utils.hadoop.hms.mirror.feature;

import com.cloudera.utils.hadoop.hms.mirror.EnvironmentTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Pattern;

public class BadTextFileDefFeature extends BaseFeature implements Feature {
    private final String ROW_FORMAT_DELIMITED = "ROW FORMAT DELIMITED";
    private final Pattern FIELDS_TERMINATED_BY = Pattern.compile("FIELDS TERMINATED BY (.*)");
    private final Pattern LINES_TERMINATED_BY = Pattern.compile("LINES TERMINATED BY (.*)");
    private final String WITH_SERDEPROPERTIES = "WITH SERDEPROPERTIES";

    private final String ROW_FORMAT_SERDE = "ROW FORMAT SERDE";
    private final String LAZY_SERDE = "'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'";

    private static final Logger LOG = LoggerFactory.getLogger(BadTextFileDefFeature.class);

    public String getDescription() {
        return "Table schema definitions that include both ROW FORMAT DELIMITED BY and " +
                "WITH SERDEPROPERTIES in the declaration aren't valid as a new schema when you " +
                "attempt to replay the schema.  This happens when tables are ALTERED with SERDEPROPERTIES " +
                "after initial creation.  This process will migrate the FIELDS TERMINATED BY and " +
                "LINES TERMINATED BY values into the SERDEPROPERTIES so the schema can be successfully created.";
    }

    @Override
    public Boolean applicable(EnvironmentTable envTable) {
        return applicable(envTable.getDefinition());
    }

    @Override
    public Boolean applicable(List<String> schema) {
        Boolean rtn = Boolean.FALSE;
        if (contains(ROW_FORMAT_DELIMITED, schema) && contains(WITH_SERDEPROPERTIES, schema)) {
            rtn = Boolean.TRUE;
        }
        return rtn;
    }


    @Override
    public Boolean fixSchema(EnvironmentTable envTable) {
        return fixSchema(envTable.getDefinition());
    }

    @Override
    /**
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '|'
     LINES TERMINATED BY '\n'
     WITH SERDEPROPERTIES (
     'escape.delim'='\\')
     STORED AS INPUTFORMAT
     'org.apache.hadoop.mapred.TextInputFormat'
     OUTPUTFORMAT
     'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
     */
    public Boolean fixSchema(List<String> schema) {
        if (applicable(schema)) {
//            schema = addEscaped(schema);
            LOG.debug("Checking if table has OLD TEXTFILE definition");
            // find the index of the ROW_FORMAT_DELIMITED
            if (contains(ROW_FORMAT_DELIMITED, schema) && contains(WITH_SERDEPROPERTIES, schema)) {
                // Bad Definition.
                // Get the value for FIELDS_TERMINATED_BY
                String ftb = getGroupFor(FIELDS_TERMINATED_BY, schema);
                if (ftb.equals("\f")) {
                    // Convert to O
                    ftb = "\014";
                }
                // Get the value for LINES_TERMINATED_BY
                String ltb = getGroupFor(LINES_TERMINATED_BY, schema);
                // Remove bad elements
                int RFD = indexOf(schema, ROW_FORMAT_DELIMITED);
                int WS = indexOf(schema, WITH_SERDEPROPERTIES);
                removeRange(RFD, WS, schema);

                schema.add(RFD++, ROW_FORMAT_SERDE);
                schema.add(RFD++, LAZY_SERDE);
                RFD++;
                if (ftb != null) {
                    schema.add(RFD++, "'field.delim'=" + ftb + ",");
                }
                if (ltb != null) {
                    schema.add(RFD++, "'line.delim'=" + ltb + ",");
                }
            }
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

}
