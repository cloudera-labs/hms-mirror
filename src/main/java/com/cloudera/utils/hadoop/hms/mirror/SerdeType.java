package com.cloudera.utils.hadoop.hms.mirror;

import java.util.ArrayList;
import java.util.List;

public enum SerdeType {
    ORC(134217728, "org.apache.hadoop.hive.ql.io.orc.OrcSerde"),
    PARQUET(134217728, "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "parquet.hive.serde.ParquetHiveSerDe"),
    TEXT(268435456, "org.apache.hadoop.hive.serde2.OpenCSVSerde",
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
            "org.apache.hadoop.hive.serde2.JsonSerDe"),
    BINARY(134217728,"org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe",
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
    UNKNOWN(268435456);

    Integer targetSize = null;
    List<String> serdeClasses = new ArrayList<String>();

    private SerdeType(Integer targetSize, String... serdeClasses) {
        this.targetSize = targetSize;
        for (String serdeClass : serdeClasses) {
            this.serdeClasses.add(serdeClass);
        }
    }

    public Boolean isType(String serdeClass) {
        Boolean rtn = Boolean.FALSE;
        rtn = serdeClasses.contains(serdeClass);
        return rtn;
    }

}
