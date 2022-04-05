package com.cloudera.utils.hadoop.hms;

import org.junit.Before;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertTrue;

public class MirrorTestBase {

    protected final String HDP2_CDP = "default.yaml.hdp2-cdp";
    protected final String CDP_CDP = "default.yaml.cdp-cdp";
    //    private Boolean execute = Boolean.FALSE;
    protected String[] execArgs = {"-e", "--accept"};
    protected Boolean execute = Boolean.FALSE;
    protected String homedir = null;
    protected String working_db = "tpcds_bin_partitioned_orc_10";
    protected String intermediate_storage = "s3a://my_is_bucket";
    protected String common_storage = "s3a:/my_cs_bucket";
    protected String table_filter = null;

    protected void reset() {
        DateFormat df = new SimpleDateFormat("yyyymmDDHHMMSS");
        String outputDir = String.format(homedir + System.getProperty("file.separator") + "hms-mirror-reports/reset_right/" + df.format(new Date()));
        String[] args = new String[]{"-db", working_db, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP, "-rr"};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    protected String[] toExecute(String[] one, String[] two) {
        String[] rtn = one;
        if (execute) {
            rtn = new String[one.length + two.length];
            System.arraycopy(one, 0, rtn, 0, one.length);
            System.arraycopy(two, 0, rtn, one.length, two.length);
        }
        if (table_filter != null) {
            String[] tf = {"-tf", table_filter};
            String[] tfRtn = new String[rtn.length + tf.length];
            System.arraycopy(rtn, 0, tfRtn, 0, rtn.length);
            System.arraycopy(tf, 0, tfRtn, rtn.length, tf.length);
            rtn = tfRtn;
        }
        return rtn;
    }

    @Before
    public void setUp() throws Exception {
        homedir = System.getProperty("user.home");

        // Override default db for test run.
        String lclWorkingDb = System.getenv("DB");
        if (lclWorkingDb != null) {
            working_db = lclWorkingDb;
        }

        // Set the execute flags for test suite.
        String execStr = System.getenv("EXECUTE");
        if (execStr != null) {
            execute = Boolean.valueOf(execStr);
        }

        String tfStr = System.getenv("TF");
        if (tfStr != null) {
            table_filter = tfStr;
        }

        // Override intermediate_storage for test run.
        String lclIntermediate_storage = System.getenv("IS");
        if (lclIntermediate_storage != null) {
            intermediate_storage = lclIntermediate_storage;
        }

        // Override intermediate_storage for test run.
        String lclcommon_storage = System.getenv("CS");
        if (lclcommon_storage != null) {
            common_storage = lclcommon_storage;
        }

    }


}
