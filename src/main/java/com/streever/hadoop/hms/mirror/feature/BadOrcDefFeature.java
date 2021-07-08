package com.streever.hadoop.hms.mirror.feature;

import com.streever.hadoop.hms.mirror.EnvironmentTable;
import com.streever.hadoop.hms.util.TableUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class BadOrcDefFeature implements Feature {
    private final String ROW_FORMAT_DELIMITED = "ROW FORMAT DELIMITED";
    private final String STORED_AS_INPUTFORMAT = "STORED AS INPUTFORMAT";
    private final String OUTPUTFORMAT = "OUTPUTFORMAT";
    private final String ROW_FORMAT_SERDE = "ROW FORMAT SERDE";
    private final String LAZY_SERDE = "'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'";
    private final String ORC_SERDE = "  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'";
    private static Logger LOG = LogManager.getLogger(BadOrcDefFeature.class);

    @Override
    public EnvironmentTable fixSchema(EnvironmentTable envTable) {
        List<String> fixed = fixSchema(envTable.getDefinition());
        envTable.setDefinition(fixed);
        return envTable;
    }

    @Override
    /**
     * ROW FORMAT DELIMITED
     *   FIELDS TERMINATED BY '\t'
     *   LINES TERMINATED BY '\n'
     * STORED AS INPUTFORMAT
     *   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     * OUTPUTFORMAT
     *   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     */
    public List<String> fixSchema(List<String> schema) {
        List<String> rtn = new ArrayList<String>();
        rtn.addAll(schema);
        LOG.debug("Checking if table has bad ORC definition");
        // find the index of the ROW_FORMAT_DELIMITED
        int rfdIdx = indexOf(rtn, ROW_FORMAT_DELIMITED);
        if (rfdIdx > 0) {
            // Find the "STORED AS INPUTFORMAT" index
            int saiIdx = indexOf(rtn, STORED_AS_INPUTFORMAT);
            if (saiIdx > rfdIdx) {
                if (rtn.get(saiIdx + 1).trim().equals("'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'")) {
                    int of = indexOf(rtn, OUTPUTFORMAT);
                    if (of > saiIdx + 1) {
                        if (rtn.get(of + 1).trim().equals("'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'")) {
                            LOG.debug("BAD ORC definition found. Correcting...");
                            // All matches.  Need to replace with serde
                            for (int i = saiIdx - 1; i >= rfdIdx; i--) {
                                rtn.remove(i);
                            }
                            rtn.add(rfdIdx, ORC_SERDE);
                            rtn.add(rfdIdx, ROW_FORMAT_SERDE);
                        }
                    }
                }
            }
        }
        return rtn;
    }

    private int indexOf(List<String> schema, String condition) {
        int i = 0;
        for (String line : schema) {
            if (line.trim().equalsIgnoreCase(condition)) {
                break;
            }
            i++;
        }
        return i;
    }
}
