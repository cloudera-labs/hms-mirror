package com.cloudera.utils.hadoop.hms;

import org.junit.Test;

import static com.cloudera.utils.hadoop.hms.EnvironmentConstants.*;
import static org.junit.Assert.assertEquals;

public class EndToEndCDPtoCDPTest extends EndToEndBase {

    @Test
    public void test_001 () {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-smn", "ofs://OHOME90",
                "-epl",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void test_002 () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
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
    public void test_003 () {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-ma",
                "-epl",
                "-ltd", ASSORTED_TBLS_04, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void test_004 () {
        // Issues:
        /*
        FIXED: 1. Warehouse Locations not being set.
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-ma",
                "-wd", "/finance/mngd-hive",
                "-ewd", "/finance/ext-hive",
                "-epl",
                "-ltd", ASSORTED_TBLS_04, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

}
