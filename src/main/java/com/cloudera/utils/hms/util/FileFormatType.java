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

package com.cloudera.utils.hms.util;

import java.util.ArrayList;
import java.util.List;

public enum FileFormatType {
    ORC("org.apache.hadoop.hive.ql.io.orc.OrcSerde", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),
    TEXTFILE("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "org.apache.hadoop.mapred.TextInputFormat", "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
    SEQUENCEFILE("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "org.apache.hadoop.mapred.SequenceFileInputFormat", "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
    PARQUET("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
    JSONFILE("org.apache.hadoop.hive.serde2.JsonSerDe", "org.apache.hadoop.mapred.TextInputFormat", "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
    RCFILE("org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe", "org.apache.hadoop.hive.ql.io.RCFileInputFormat", "org.apache.hadoop.hive.ql.io.RCFileOutputFormat"),
    AVRO("org.apache.hadoop.hive.serde2.avro.AvroSerDe", "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat", "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"),
    UNKNOWN("unknown", "unknown", "unknown");

    private final String rowFormatSerde;
    private final String inputFormat;
    private final String outputFormat;

    FileFormatType(String rowFormatSerde, String inputFormat, String outputFormat) {
        this.rowFormatSerde = rowFormatSerde;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
    }

    public static FileFormatType from(String rowFormatSerde, String inputFormat) {
        FileFormatType rtn = UNKNOWN;
        // Remove Quotes if they exists.
        List<FileFormatType> fileFormatTypeList = new ArrayList<FileFormatType>();

        String inputFormatLcl = inputFormat;
        if (inputFormatLcl.startsWith("'")) {
            inputFormatLcl = inputFormat.substring(1, inputFormat.length() - 1);
        }
        String rowFormatSerdeLcl = rowFormatSerde;
        if (rowFormatSerdeLcl.startsWith("'")) {
            rowFormatSerdeLcl = rowFormatSerde.substring(1, rowFormatSerde.length() - 1);
        }
        if (rowFormatSerdeLcl != null) {
            for (FileFormatType storageType : FileFormatType.values()) {
                if (storageType.rowFormatSerde.equals(rowFormatSerdeLcl)) {
                    fileFormatTypeList.add(storageType);
//                    rtn = storageType;
//                    break;
                }
            }
        }

        for (FileFormatType storageType : fileFormatTypeList) {
            if (storageType.inputFormat.equals(inputFormatLcl)) {
                rtn = storageType;
                break;
            }
        }
        return rtn;
    }
}
