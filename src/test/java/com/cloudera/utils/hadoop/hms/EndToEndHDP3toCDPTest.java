package com.cloudera.utils.hadoop.hms;

import org.junit.Test;

import static com.cloudera.utils.hadoop.hms.EnvironmentConstants.HDP3_CDP;
import static com.cloudera.utils.hadoop.hms.EnvironmentConstants.LEGACY_MNGD_PARTS_01;
import static org.junit.Assert.assertEquals;

public class EndToEndHDP3toCDPTest extends EndToEndBase {

    @Test
    public void sm() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-ltd", LEGACY_MNGD_PARTS_01,
                "-cfg", HDP3_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

}
