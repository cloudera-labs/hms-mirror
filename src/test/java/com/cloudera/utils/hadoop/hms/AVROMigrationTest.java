package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.MirrorConf;
import com.cloudera.utils.hadoop.hms.mirror.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static com.cloudera.utils.hadoop.hms.TestSQL.*;
import static com.cloudera.utils.hadoop.hms.TestSQL.TBL_INSERT;
import static org.junit.Assert.assertTrue;

public class AVROMigrationTest  extends MirrorTestBase {
    @AfterClass
    public static void tearDownClass() throws Exception {
        dataCleanup(DATACLEANUP.BOTH);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DataState.getInstance().setConfiguration(CDP_CDP);
        dataSetupAvro();
    }

    @After
    public void tearDown() throws Exception {
        dataCleanup(DATACLEANUP.RIGHT);
    }


    public Boolean dataSetupAvro() {
        if (!DataState.getInstance().isDataCreated()) {
            String nameofCurrMethod = new Throwable()
                    .getStackTrace()[0]
                    .getMethodName();

            String outputDir = outputDirBase + nameofCurrMethod;

            String[] args = new String[]{"-d", "STORAGE_MIGRATION", "-smn", "s3a://something_not_relevant",
                    "-wd", "/hello", "-ewd", "/hello-ext",
                    "-db", DataState.getInstance().getWorking_db(), "-o", outputDir,
                    "-cfg", DataState.getInstance().getConfiguration()};
            args = toExecute(args, execArgs, Boolean.TRUE);

            List<Pair> leftSql = new ArrayList<Pair>();
            build_use_db(leftSql);

            List<String[]> dataset = null;
            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
                dataset = getDataset(4, 200, null);
            }
            build_n_populate(CREATE_AVRO_TBL_SHORT, TBL_INSERT, null, leftSql, new String[]{"avro_01", "hdfs://HDP50/user/dstreev/datasets/avro/test.avsc"});

            Mirror cfgMirror = new Mirror();
            long rtn = cfgMirror.setupSqlLeft(args, leftSql);
            DataState.getInstance().setDataCreated(Boolean.TRUE);
        }
        return Boolean.TRUE;
    }

    @Test
    public void test_avro_01 () {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY", "-db", DataState.getInstance().getWorking_db(),
                "-asm",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }
}
