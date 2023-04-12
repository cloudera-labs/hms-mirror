package com.cloudera.utils.hadoop.hms.util;

import java.util.ArrayList;
import java.util.List;

public enum StorageType {
    ORC("org.apache.hadoop.hive.ql.io.orc.OrcSerde","org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),
    TEXTFILE("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe","org.apache.hadoop.mapred.TextInputFormat", "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
    SEQUENCEFILE("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe","org.apache.hadoop.mapred.SequenceFileInputFormat", "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
    PARQUET("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe","org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
    JSONFILE("org.apache.hadoop.hive.serde2.JsonSerDe","org.apache.hadoop.mapred.TextInputFormat","org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
    RCFILE("org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe","org.apache.hadoop.hive.ql.io.RCFileInputFormat","org.apache.hadoop.hive.ql.io.RCFileOutputFormat"),
    AVRO("org.apache.hadoop.hive.serde2.avro.AvroSerDe","org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat", "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"),
    UNKNOWN("unknown", "unknown", "unknown");

    private String rowFormatSerde;
    private String inputFormat;
    private String outputFormat;

    StorageType(String rowFormatSerde, String inputFormat, String outputFormat) {
        this.rowFormatSerde = rowFormatSerde;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
    }

    public static StorageType from(String rowFormatSerde, String inputFormat) {
        StorageType rtn = UNKNOWN;
        // Remove Quotes if they exists.
        List<StorageType> storageTypeList = new ArrayList<StorageType>();

        String inputFormatLcl = inputFormat;
        if (inputFormatLcl.startsWith("'")) {
            inputFormatLcl = inputFormat.substring(1, inputFormat.length() - 1);
        }
        String rowFormatSerdeLcl = rowFormatSerde;
        if (rowFormatSerdeLcl.startsWith("'")) {
            rowFormatSerdeLcl = rowFormatSerde.substring(1, rowFormatSerde.length() - 1);
        }
        if (rowFormatSerdeLcl != null) {
            for (StorageType storageType : StorageType.values()) {
                if (storageType.rowFormatSerde.equals(rowFormatSerdeLcl)) {
                    storageTypeList.add(storageType);
//                    rtn = storageType;
//                    break;
                }
            }
        }

        for (StorageType storageType : storageTypeList) {
            if (storageType.inputFormat.equals(inputFormatLcl)) {
                rtn = storageType;
                break;
            }
        }
        return rtn;
    }
}
