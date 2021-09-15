package com.streever.hadoop.hms.mirror.feature;

import com.streever.hadoop.hms.mirror.EnvironmentTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class BadRCDefFeature extends BaseFeature implements Feature {
    private final String ROW_FORMAT_DELIMITED = "ROW FORMAT DELIMITED";
    private final String STORED_AS_INPUTFORMAT = "STORED AS INPUTFORMAT";
    private final String STORED_AS_RCFILE = "STORED AS RCFile";
    private final String OUTPUTFORMAT = "OUTPUTFORMAT";
    private final String ROW_FORMAT_SERDE = "ROW FORMAT SERDE";
    private final String LAZY_SERDE = "'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'";
    private final String ORC_SERDE = "  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'";
    private final String RC_INPUT_SERDE = "  'org.apache.hadoop.hive.ql.io.RCFileInputFormat'";
    private final String RC_OUTPUT_SERDE = "  'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'";
    private static Logger LOG = LogManager.getLogger(BadRCDefFeature.class);

    @Override
    public Boolean applicable(EnvironmentTable envTable) {
        return applicable(envTable.getDefinition());
    }

    @Override
    public Boolean applicable(List<String> schema) {
        Boolean rtn = Boolean.FALSE;

        int rfdIdx = indexOf(schema, ROW_FORMAT_DELIMITED);
        if (rfdIdx > 0) {
            // Find the "STORED AS INPUTFORMAT" index
            int saiIdx = indexOf(schema, STORED_AS_INPUTFORMAT);
            if (saiIdx > rfdIdx) {
                if (schema.get(saiIdx + 1).trim().equals(RC_INPUT_SERDE.trim())) {
                    int of = indexOf(schema, OUTPUTFORMAT);
                    if (of > saiIdx + 1) {
                        if (schema.get(of + 1).trim().equals(RC_OUTPUT_SERDE.trim())) {
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
     * ROW FORMAT DELIMITED
     *   FIELDS TERMINATED BY '|'
     * STORED AS INPUTFORMAT
     *   'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
     * OUTPUTFORMAT
     *   'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'
     */
    public List<String> fixSchema(List<String> schema) {
        List<String> rtn = addEscaped(schema);
        LOG.debug("Checking if table has bad ORC definition");
        // find the index of the ROW_FORMAT_DELIMITED
        int rfdIdx = indexOf(rtn, ROW_FORMAT_DELIMITED);
        if (rfdIdx > 0) {
            // Find the "STORED AS INPUTFORMAT" index
            int saiIdx = indexOf(rtn, STORED_AS_INPUTFORMAT);
            if (saiIdx > rfdIdx) {
                if (rtn.get(saiIdx + 1).trim().equals(RC_INPUT_SERDE.trim())) {
                    int of = indexOf(rtn, OUTPUTFORMAT);
                    if (of > saiIdx + 1) {
                        if (rtn.get(of + 1).trim().equals(RC_OUTPUT_SERDE.trim())) {
                            LOG.debug("BAD RC definition found. Correcting...");
                            // All matches.  Need to replace with serde
                            for (int i = of + 1; i >= rfdIdx; i--) {
                                rtn.remove(i);
                            }
                            rtn.add(rfdIdx, STORED_AS_RCFILE);
                        }
                    }
                }
            }
        }
        return rtn;
    }

}
