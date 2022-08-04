package com.cloudera.utils.hadoop.hms.mirror.feature;

import com.cloudera.utils.hadoop.hms.mirror.EnvironmentTable;
import com.cloudera.utils.hadoop.hms.util.TableUtils;

import java.util.List;

public class SparkSqlPartFeature extends BaseFeature implements Feature {

    private static final String SPARK_SCHEMA_PROPERTY = "spark.sql.sources.schema.part";

    @Override
    public Boolean applicable(EnvironmentTable envTable) {
        return applicable(envTable.getDefinition());
    }

    @Override
    public Boolean applicable(List<String> schema) {
        Boolean rtn = Boolean.FALSE;
        for (int i=0;i<10;i++) {
            String key = SPARK_SCHEMA_PROPERTY + "." + i;
            String value = TableUtils.getTblProperty(key, schema);
            if (value != null && value.contains("\\")) {
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
            for (int i=0;i<10;i++) {
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
