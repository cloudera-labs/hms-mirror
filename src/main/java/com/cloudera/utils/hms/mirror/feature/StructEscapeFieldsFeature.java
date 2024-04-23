/*
 * Copyright (c) 2022-2024. Cloudera, Inc. All Rights Reserved
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

/*
Review the fields of a table for "key/reserved" words.  If they are present, check to see if they've been
escaped and if not escape the field name and post a WARNING that a reserved work was found in the tables
field definition.
 */
@Slf4j
public class StructEscapeFieldsFeature extends BaseFeature implements Feature {
    private final String CREATE = "CREATE";
    private final String PARTITIONED_BY = "PARTITIONED BY";
    private final String CLUSTERED_BY = "CLUSTERED BY";
    private final String SKEWED_BY = "SKEWED BY";
    private final String ROW_FORMAT = "ROW FORMAT";
    private final String STORED_AS = "STORED AS";
    private final String ESCAPE = "`";
    private final String STRUCT = ".*?struct.*";

    @Override
    public Boolean applicable(EnvironmentTable envTable) {
        return applicable(envTable.getDefinition());
    }

    @Override
    public Boolean applicable(List<String> schema) {
        Boolean rtn = Boolean.FALSE;
        int cIdx = indexOf(schema, CREATE);
        int eIdx = indexOf(schema, PARTITIONED_BY);
        if (eIdx == -1)
            eIdx = indexOf(schema, CLUSTERED_BY);
        if (eIdx == -1)
            eIdx = indexOf(schema, SKEWED_BY);
        if (eIdx == -1)
            eIdx = indexOf(schema, ROW_FORMAT);
        if (eIdx == -1)
            eIdx = indexOf(schema, STORED_AS);

        for (int i = cIdx + 1; i < eIdx; i++) {
            String checkValue = schema.get(i).toLowerCase();
            if (checkValue.matches(STRUCT)) {
                // Found a Struct Type.
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    @Override
    public Boolean fixSchema(List<String> schema) {
        if (applicable(schema)) {
            int cIdx = indexOf(schema, CREATE);
            int eIdx = indexOf(schema, PARTITIONED_BY);
            if (eIdx == -1)
                eIdx = indexOf(schema, CLUSTERED_BY);
            if (eIdx == -1)
                eIdx = indexOf(schema, SKEWED_BY);
            if (eIdx == -1)
                eIdx = indexOf(schema, ROW_FORMAT);
            if (eIdx == -1)
                eIdx = indexOf(schema, STORED_AS);

            for (int i = cIdx + 1; i < eIdx; i++) {
                if (schema.get(i).toLowerCase().matches(STRUCT)) {
                    // Found a Struct Type.
                    String[] fieldParts = schema.get(i).trim().split(" ");
                    if (fieldParts.length == 2) {
                        //
                        String correctedType = fixStruct(fieldParts[1]);
                        schema.set(i, fieldParts[0] + " " + correctedType);
                    }
                }
            }
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    @Override
    public Boolean fixSchema(EnvironmentTable envTable) {
        return fixSchema(envTable.getDefinition());
    }

    public String fixStruct(String complexTypeDef) {
        String[] parts = complexTypeDef.split(":");
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            int lComma = part.lastIndexOf(",");
            int llt = part.lastIndexOf("<");
            int marker = Math.max(lComma, llt);
            if (marker > -1) {
                String fieldName = part.substring(marker + 1);
                if (fieldName.startsWith(ESCAPE) && fieldName.endsWith(ESCAPE)) {
                    // nothing to do.
                    sb.append(part).append(":");
                } else if (!part.equals(parts[parts.length - 1])) {
                    sb.append(part, 0, marker + 1).append(ESCAPE).append(fieldName).append(ESCAPE).append(":");
                } else {
                    sb.append(part);
                }
            } else {
                sb.append(part);
            }
        }
        String struct2 = sb.toString();
        String fixedStruct = struct2;
        return fixedStruct;
    }

    @Override
    public String getDescription() {
        return "Reserved/Key words in a tables field definition need to be escaped.  If they are NOT, this process " +
                "will escape them and post a warning about the change.";
    }

}
