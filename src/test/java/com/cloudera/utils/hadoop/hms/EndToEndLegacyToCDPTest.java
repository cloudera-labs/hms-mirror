package com.cloudera.utils.hadoop.hms;

import org.junit.Test;

import static com.cloudera.utils.hadoop.hms.EnvironmentConstants.*;
import static org.junit.Assert.assertEquals;

public class EndToEndLegacyToCDPTest extends EndToEndBase {

    @Test
    public void test_010 () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void test_012 () {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-ma",
                "-epl",
                "-ltd", ASSORTED_TBLS_04, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void test_013 () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "DUMP",
                "-ma",
                "-epl",
                "-ltd", ASSORTED_TBLS_04, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void test_014 () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-ma",
                "-epl",
                "-ltd", ASSORTED_TBLS_04, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void test_015 () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-ma",
                "-epl",
                "-ltd", ASSORTED_TBLS_04, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void acid_w_parts_so_dc () {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-ma",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-dc",
                "-ltd", ACID_W_PARTS_05, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 1, rtn);
    }

    @Test
    public void acid_w_parts_sql () {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-ma",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-ltd", ACID_W_PARTS_05, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void acid_w_parts_so () {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-ma",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-ltd", ACID_W_PARTS_05, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }


    @Test
    public void so_ext_purge_w_wd_epl_odd_parts () {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-ma",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so_ext_purge_w_wd_epl_rdl_odd_parts () {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-ma",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-rdl",
                "-epl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 1, rtn);
    }

    @Test
    public void sql_ext_purge_dc_err () {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 4294967296l, rtn);
    }


    @Test
    public void so_legacy_mngd_w_parts () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so_legacy_mngd_w_parts_dc () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so_legacy_mngd_w_parts_wd_dc () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so_legacy_mngd_w_parts_wd_epl_dc () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so_legacy_mngd_w_parts_wd_epl_dc_tf () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-tf", "web_sales",
                "-epl",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void linked_legacy_mngd_w_parts_wd_epl_dc () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "LINKED",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 8388608l, rtn);
    }

    @Test
    public void linked_legacy_mngd_w_parts_wd_epl () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "LINKED",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void linked_ext_purge_w_odd_parts_wd_epl () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "LINKED",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void test_102 () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-epl",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void test_103 () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-epl",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void test_104 () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-epl",
                "-wd", "/warehouse/tablespace/managed/hive",
                "-ewd", "/warehouse/tablespace/external/hive",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void test_105 () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-epl",
                "-wd", "/warehouse/tablespace/managed/hive",
                "-ewd", "/warehouse/tablespace/external/hive",
                "-rdl",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 1, rtn);
    }

    @Test
    public void test_106 () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-epl",
                "-wd", "/warehouse/tablespace/managed/hive",
                "-ewd", "/warehouse/tablespace/external/hive",
                "-rdl",
                "-dc",
                "-glm", "/apps/hive/warehouse=/warehouse/tablespace/external/hive",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

}
