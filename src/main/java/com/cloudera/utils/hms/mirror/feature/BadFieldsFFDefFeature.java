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
import java.util.regex.Pattern;

@Slf4j
public class BadFieldsFFDefFeature extends BaseFeature implements Feature {
    private final String FTB = "FIELDS TERMINATED BY";
    private final Pattern FIELDS_TERMINATED_BY = Pattern.compile(FTB + " '(.*)'");

    @Override
    public Boolean applicable(EnvironmentTable envTable) {
        return applicable(envTable.getDefinition());
    }

    @Override
    /*
    check if '\f' is used in FIELDS TERMINATED BY.
     */
    public Boolean applicable(List<String> schema) {
        Boolean rtn = Boolean.FALSE;

        String v = getGroupFor(FIELDS_TERMINATED_BY, schema);
        try {
            if (v != null && 'f' == v.charAt(1)) {
                rtn = Boolean.TRUE;
            }
        } catch (Throwable t) {
            // skip
        }

        return rtn;
    }

    @Override
    /*
    replace '\f' with '\014' in FIELD TERMINATED BY because HIVE won't take this.
     */
    public Boolean fixSchema(EnvironmentTable envTable) {
        return fixSchema(envTable.getDefinition());
    }

    @Override
    /*
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '\f'

     The '\f' won't translate in schemas that are replayed.  It needs to be converted
     to '\014' and then hive will translate it to '\f'.
     */
    public Boolean fixSchema(List<String> schema) {
        if (applicable(schema)) {
            log.debug("Checking if table has Bad Fields definition");
            // find the index of the ROW_FORMAT_DELIMITED

            String v = getGroupFor(FIELDS_TERMINATED_BY, schema);


            Boolean badDef = Boolean.FALSE;
            try {
                if (v != null && 'f' == v.charAt(1)) {
                    badDef = Boolean.TRUE;
                }
            } catch (Throwable t) {
                // skip
            }

            if (badDef) {
                // Find location.
                int loc = -1;
                Boolean found = Boolean.FALSE;
                for (String line : schema) {
                    loc++;
                    if (line.trim().startsWith(FTB)) {
                        found = Boolean.TRUE;
                        break;
                    }
                }
                if (found) {
                    schema.set(loc, FTB + " '\\014'");
                }
            }
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public String getDescription() {
        return "Table schemas with a \\f definition in the FIELDS TERMINATED BY declaration will not be created correctly " +
                "in Hive.  We need to set the value to the character set value \\014 to successfully translate the schema.";
    }

}
