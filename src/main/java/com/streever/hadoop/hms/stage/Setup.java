package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.mirror.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Setup {
    private static Logger LOG = LogManager.getLogger(Setup.class);

    private Config config = null;
    private Conversion conversion = null;

    public Setup(Config config, Conversion conversion) {
        this.config = config;
        this.conversion = conversion;
    }

    public void run() {
        Date startTime = new Date();
        LOG.info("GATHERING METADATA: Start Processing for databases: " + Arrays.toString((config.getDatabases())));

        List<ScheduledFuture> gtf = new ArrayList<ScheduledFuture>();
        for (String database : config.getDatabases()) {
            DBMirror dbMirror = conversion.addDatabase(database);
            GetTables gt = new GetTables(config, dbMirror);
            gtf.add(config.getMetadataThreadPool().schedule(gt, 1, TimeUnit.MILLISECONDS));
        }

        CreateDatabases createDatabases = new CreateDatabases(config);
        gtf.add(config.getMetadataThreadPool().schedule(createDatabases, 1, TimeUnit.MILLISECONDS));

        // When the tables have been gathered, complete the process for the METADATA Stage.
        while (true) {
            boolean check = true;
            for (ScheduledFuture sf : gtf) {
                if (!sf.isDone()) {
                    check = false;
                    break;
                }
            }
            if (check)
                break;
        }

        LOG.info(">>>>>>>>>>> Getting Table Metadata");
        List<ScheduledFuture> tmdf = new ArrayList<ScheduledFuture>();
        Set<String> collectedDbs = conversion.getDatabases().keySet();
        for (String database : collectedDbs) {
            DBMirror dbMirror = conversion.getDatabase(database);
            Set<String> tables = dbMirror.getTableMirrors().keySet();
            for (String table : tables) {
                TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
//                    if (!tblMirror.isTransactional()) {
                GetTableMetadata tmd = new GetTableMetadata(config, dbMirror, tblMirror);
                tmdf.add(config.getMetadataThreadPool().schedule(tmd, 1, TimeUnit.MILLISECONDS));
//                    }
            }
        }

        while (true) {
            boolean check = true;
            for (ScheduledFuture sf : tmdf) {
                if (!sf.isDone()) {
                    check = false;
                    break;
                }
            }
            if (check)
                break;
        }

        LOG.info("==============================");
        LOG.info(conversion.toString());
        LOG.info("==============================");
        Date endTime = new Date();
        DecimalFormat df = new DecimalFormat("#.###");
        df.setRoundingMode(RoundingMode.CEILING);
        LOG.info("GATHERING METADATA: Completed in " + df.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");

    }

}
