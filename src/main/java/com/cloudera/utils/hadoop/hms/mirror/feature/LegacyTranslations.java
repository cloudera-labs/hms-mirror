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
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.cloudera.utils.hadoop.hms.util.TableUtils.*;

public class LegacyTranslations extends BaseFeature implements Feature {
    private static final Logger LOG = LoggerFactory.getLogger(LegacyTranslations.class);

    private final Pattern RFS = Pattern.compile(ROW_FORMAT_SERDE + " '(.*)'");
    private final Pattern SAIF = Pattern.compile(STORED_AS_INPUTFORMAT + " '(.*)'");
    private final Pattern SAOF = Pattern.compile(OUTPUTFORMAT + " '(.*)'");

    private Map<String, String> rowSerde = null;

    // TODO: When needed... add translation for formats.
    //    private Map<String, String> inputFormat = null;
    //    private Map<String, String> outputFormat = null;

    public Map<String, String> getRowSerde() {
        if (rowSerde == null) {
            rowSerde = new TreeMap<String, String>();
            rowSerde.put("'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'", "'org.apache.hadoop.hive.serde2.RegexSerDe'");
            rowSerde.put("'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'", "'org.apache.hadoop.hive.serde2.MultiDelimitSerDe'");
            rowSerde.put("'org.apache.hadoop.hive.contrib.serde2.TypedBytesSerDe'", "'org.apache.hadoop.hive.serde2.TypedBytesSerDe'");
        }
        return rowSerde;
    }

    public void setRowSerde(Map<String, String> rowSerde) {
        this.rowSerde = rowSerde;
    }

//    public Map<String, String> getInputFormat() {
//        return inputFormat;
//    }
//
//    public void setInputFormat(Map<String, String> inputFormat) {
//        this.inputFormat = inputFormat;
//    }
//
//    public Map<String, String> getOutputFormat() {
//        return outputFormat;
//    }
//
//    public void setOutputFormat(Map<String, String> outputFormat) {
//        this.outputFormat = outputFormat;
//    }

    @Override
    public Boolean applicable(EnvironmentTable envTable) {
        return applicable(envTable.getDefinition());
    }

    @Override
    public Boolean applicable(List<String> schema) {
        Boolean rtn = Boolean.FALSE;

        int rfdIdx = indexOf(schema, ROW_FORMAT_SERDE);
        if (rfdIdx > 0) {
            rtn = Boolean.TRUE;
        }

        return rtn;
    }

    @Override
    public Boolean fixSchema(List<String> schema) {
        Boolean rtn = Boolean.FALSE;
        if (applicable(schema)) {
            // find the index of the ROW_FORMAT_DELIMITED
            int rfdIdx = indexOf(schema, ROW_FORMAT_SERDE);
            if (rfdIdx > 0) {
                String serde = schema.get(rfdIdx + 1);
                if (getRowSerde().containsKey(serde.trim())) {
                    schema.set(rfdIdx+1, getRowSerde().get(serde.trim()));
                    rtn = Boolean.TRUE;
                }
            }
        }
        return rtn;
    }

    public Boolean fixSchema(EnvironmentTable environmentTable) {
        return fixSchema(environmentTable.getDefinition());
    }

    @Override
    @JsonIgnore
    public String getDescription() {
        return "Legacy Translations";
    }

    protected String getGroupFor(Pattern pattern, List<String> definition) {
        String rtn = null;
        for (String line : definition) {
//            String adjLine = StringEscapeUtils.escapeJava(line);
            Matcher m = pattern.matcher(line);
            if (m.find()) {
                rtn = m.group(1);
            }
        }
        return rtn;
    }

}
