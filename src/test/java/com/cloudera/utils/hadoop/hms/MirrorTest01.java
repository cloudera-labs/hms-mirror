package com.cloudera.utils.hadoop.hms;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class MirrorTest01 extends MirrorTestBase {

    @Test
    public void test_spot_test() {
//        reset();
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = homedir + separator + "hms-mirror-reports" + separator + getClass().getSimpleName() +
                separator + nameofCurrMethod;

//        String[] args = new String[]{"-d", "SCHEMA_ONLY", "-db", working_db, "-ma", "6", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
//        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-mao", "4", "-da", "-r", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
//        String[] args = new String[]{"-d", "EXPORT_IMPORT", "-db", working_db, "-mao", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + CDP_CDP};
//        String[] args = new String[]{"-db", working_db, "-ma", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(), "-d", "STORAGE_MIGRATION",
                "-smn", "ofs://OHOME90",
                "-f",
                "-wd", "/warehouse/managed",
                "-ewd", "/warehouse/external",
                "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure: " + rtn, rtn == 0);
    }

}
