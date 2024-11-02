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

package com.cloudera.utils.hms.mirror.domain.support;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Getter
public enum SerdeType {
    ORC(134217728, Boolean.TRUE, "org.apache.hadoop.hive.ql.io.orc.OrcSerde"),
    PARQUET(134217728, Boolean.TRUE, "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "parquet.hive.serde.ParquetHiveSerDe"),
    TEXT(268435456, Boolean.FALSE, "org.apache.hadoop.hive.serde2.OpenCSVSerde",
            "org.apache.hadoop.mapred.TextInputFormat"),
    AVRO(134217728, Boolean.TRUE,
            "org.apache.hadoop.hive.serde2.avro.AvroSerDe"),
    JSON(134217728, Boolean.FALSE, "org.apache.hadoop.hive.serde2.JsonSerDe"),
    SEQUENCE_FILE(134217728, Boolean.FALSE, "org.apache.hadoop.mapred.SequenceFileInputFormat"),
    BINARY(134217728, Boolean.FALSE, "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe",
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
    UNKNOWN(268435456, Boolean.FALSE);

    Integer targetSize = null;
    final List<String> serdeClasses = new ArrayList<String>();
    final boolean icebergSupport = Boolean.FALSE;

    SerdeType(Integer targetSize, boolean icebergSupport, String... serdeClasses) {
        this.targetSize = targetSize;
        Collections.addAll(this.serdeClasses, serdeClasses);

    }

    public Boolean isType(String serdeClass) {
        Boolean rtn = Boolean.FALSE;
        rtn = serdeClasses.contains(serdeClass);
        return rtn;
    }

}
