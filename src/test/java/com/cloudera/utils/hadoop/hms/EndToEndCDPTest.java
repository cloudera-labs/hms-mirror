package com.cloudera.utils.hadoop.hms;

import org.junit.Test;

import static com.cloudera.utils.hadoop.hms.EnvironmentConstants.*;
import static org.junit.Assert.assertEquals;

public class EndToEndCDPTest extends EndToEndBase {


    @Test
    public void sm_smn_wd_epl_dc_mismatch() {
        /*
        This test uses a configuration (LEFT hcfs namespace) that doesn't match the table/partition prefixes. This should
        fail with warnings.

        FIXED:1. when namespace in table doesn't match the namespace specified in the hcfsNamespace, nothing is translated.
            - This should result in an error and warnings about why this didn't work.

         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-smn", "ofs://OHOME90",
                "-epl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 1, rtn);
    }

    @Test
    public void sm_smn_wd_epc_dc() {
        /*
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-smn", "ofs://OHOME90",
                "-epl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void sm_smn_wd_epl_glm_dc() {
        /*
        Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.

         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-smn", "ofs://OHOME90",
                "-epl",
                "-glm", "/user/dstreev/datasets/alt-locations/load_web_sales=/finance/external-fso/load_web_sales,/warehouse/tablespace/external/hive=/finance/external-fso",
//                "-rdl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void sm_smn_wd_epl_dc() {
        /*
        Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.

         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-smn", "ofs://OHOME90",
                "-epl",
//                "-rdl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void sm_smn_wd_epl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-smn", "ofs://OHOME90",
                "-epl",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }


}
