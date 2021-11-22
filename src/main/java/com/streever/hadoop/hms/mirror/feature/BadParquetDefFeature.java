package com.streever.hadoop.hms.mirror.feature;

import com.streever.hadoop.hms.mirror.EnvironmentTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;

public class BadParquetDefFeature extends BaseFeature implements Feature {
    private final String ROW_FORMAT_DELIMITED = "ROW FORMAT DELIMITED";
    private final String STORED_AS_INPUTFORMAT = "STORED AS INPUTFORMAT";
    private final String OUTPUTFORMAT = "OUTPUTFORMAT";
    private final String ROW_FORMAT_SERDE = "ROW FORMAT SERDE";
    private final String ROW_FORMAT_SERDE_CLASS = "'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'";
    private final String INPUT_FORMAT_CLASS = "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'";
    private final String OUTPUT_FORMAT_CLASS = "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'";
    private final String STORED_AS_PARQUET = "STORED AS PARQUET";
    private static Logger LOG = LogManager.getLogger(BadParquetDefFeature.class);

    public String getDescription() {
        return "Table schema definitions for Parquet files that don't include include INPUT and " +
                "OUTPUT format but no ROW FORMAT SERDE will not translate correctly.  This process will " +
                "remove the invalid declarations and set STORED AS PARQUET";
    }


    @Override
    public Boolean applicable(EnvironmentTable envTable) {
        return applicable(envTable.getDefinition());
    }

    @Override
    public Boolean applicable(List<String> schema) {
        Boolean rtn = Boolean.FALSE;

        int saiIdx = indexOf(schema, STORED_AS_INPUTFORMAT);
        if (saiIdx > 0) {
            // Find the "STORED AS INPUTFORMAT" index
            if (schema.get(saiIdx + 1).trim().equals(INPUT_FORMAT_CLASS)) {
                int of = indexOf(schema, OUTPUTFORMAT);
                if (of > saiIdx + 1) {
                    // Now check for OUTPUT class match
                    if (schema.get(of + 1).trim().equals(OUTPUT_FORMAT_CLASS)) {
                        // Check for Serde Class. If not present, we need to fix.
                        int rowFtSrdClz = indexOf(schema, ROW_FORMAT_SERDE_CLASS);
                        if (rowFtSrdClz == 0) {
                            rtn = Boolean.TRUE;
                        }
                    }
                }
            }
        }

        return rtn;
    }

    @Override
    public EnvironmentTable fixSchema(EnvironmentTable envTable) {
        List<String> fixed = fixSchema(envTable.getDefinition());
        envTable.setDefinition(fixed);
        return envTable;
    }

    @Override
    /**
     STORED AS INPUTFORMAT
     'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
     OUTPUTFORMAT
     'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
     */
    public List<String> fixSchema(List<String> schema) {
        List<String> rtn = addEscaped(schema);
        LOG.debug("Checking if table has bad PARQUET definition");
        // find the index of the ROW_FORMAT_DELIMITED

        int startRange = -1;
        int endRange = -1;
        int rfdIdx = indexOf(rtn, ROW_FORMAT_DELIMITED);
        if (rfdIdx > 0) {
            startRange = rfdIdx;
        }

        if (startRange == -1) {
            int rfsIdx = indexOf(rtn, ROW_FORMAT_SERDE);
            if (rfsIdx > 0) {
                startRange = rfsIdx;
            }
        }

        if (startRange == -1) {
            int saiIdx = indexOf(rtn, STORED_AS_INPUTFORMAT);
            if (saiIdx > 0) {
                startRange = saiIdx;
            }
        }

        int of = indexOf(schema, OUTPUTFORMAT);
        if (of > 0) {
            endRange = of + 2;
        }

        if ((startRange < endRange) & (startRange > 0)) {
            LOG.debug("BAD PARQUET definition found. Correcting...");
            removeRange(startRange, endRange, rtn);
            rtn.add(rfdIdx, STORED_AS_PARQUET);
        }

        return rtn;
    }

}