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

import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Slf4j
public class SparkSqlPartFeature extends BaseFeature implements Feature {

    private static final String SPARK_SCHEMA_PROPERTY = "spark.sql.sources.schema.part";

    @Override
    public Boolean applicable(EnvironmentTable envTable) {
        return applicable(envTable.getDefinition());
    }

    @Override
    public Boolean applicable(List<String> schema) {
        Boolean rtn = Boolean.FALSE;
        for (int i = 0; i < 10; i++) {
            String key = SPARK_SCHEMA_PROPERTY + "." + i;
            String value = TableUtils.getTblProperty(key, schema);
            if (!isBlank(value) && value.contains("\\")) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    @Override
    public Boolean fixSchema(List<String> schema) {
        Boolean rtn = Boolean.FALSE;
        if (applicable(schema)) {
            for (int i = 0; i < 10; i++) {
                String key = SPARK_SCHEMA_PROPERTY + "." + i;
                String value = TableUtils.getTblProperty(key, schema);

            }
        }
        return rtn;
    }

    @Override
    public Boolean fixSchema(EnvironmentTable envTable) {
        return fixSchema(envTable.getDefinition());
    }

    @Override
    public String getDescription() {
        return "Tables created by Spark will embed additional schema element in the table properties. " +
                "The extraction from Hive via 'show create table' adds escape sequences for double quotes. " +
                "This process will remove those escapes so Spark can read the schema.";
    }
}
