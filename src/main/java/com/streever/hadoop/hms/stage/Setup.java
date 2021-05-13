package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.mirror.*;
import jdk.nashorn.internal.codegen.CompilerConstants;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.RoundingMode;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;

/*
Using the config, go through the databases and tables and collect the current states.

Create the target databases, where needed to support the migration.
 */
public class Setup {
    private static Logger LOG = LogManager.getLogger(Setup.class);

    private Config config = null;
    private Conversion conversion = null;

    public Setup(Config config, Conversion conversion) {
        this.config = config;
        this.conversion = conversion;
    }

    // TODO: Need to address failures here...
    public Boolean collect() {
        Boolean rtn = Boolean.TRUE;
        Date startTime = new Date();
        LOG.info("GATHERING METADATA: Start Processing for databases: " + Arrays.toString((config.getDatabases())));

        List<ScheduledFuture<ReturnStatus>> gtf = new ArrayList<ScheduledFuture<ReturnStatus>>();
        for (String database : config.getDatabases()) {
            DBMirror dbMirror = conversion.addDatabase(database);
            try {
                config.getCluster(Environment.LEFT).getDatabase(config, dbMirror);
                config.getCluster(Environment.RIGHT).getDatabase(config, dbMirror);
            } catch (SQLException se) {
                throw new RuntimeException(se);
            }
            Callable<ReturnStatus> gt = new GetTables(config, dbMirror);
            gtf.add(config.getTransferThreadPool().schedule(gt, 1, TimeUnit.MILLISECONDS));
//            gtf.add(config.getTransferThreadPool().submit(gt));
        }

        // Check and Build DB's First.
        while (true) {
            boolean check = true;
            for (Future<ReturnStatus> sf : gtf) {
                if (!sf.isDone()) {
                    check = false;
                    break;
                }
                try {
                    if (sf.isDone() && sf.get() != null) {
                        if (sf.get().getStatus() == ReturnStatus.Status.ERROR) {
                            throw new RuntimeException(sf.get().getException());
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            if (check)
                break;
        }
        gtf.clear(); // reset

        Callable<ReturnStatus> createDatabases = new CreateDatabases(config, conversion);
        gtf.add(config.getTransferThreadPool().schedule(createDatabases, 1, TimeUnit.MILLISECONDS));

        // Check and Build DB's First.
        while (true) {
            boolean check = true;
            for (Future<ReturnStatus> sf : gtf) {
                if (!sf.isDone()) {
                    check = false;
                    break;
                }
                try {
                    if (sf.isDone() && sf.get() != null) {
                        if (sf.get().getStatus() == ReturnStatus.Status.ERROR) {
                            throw new RuntimeException(sf.get().getException());
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            if (check)
                break;
        }
        gtf.clear(); // reset

        LOG.info(">>>>>>>>>>> Getting Table Metadata");
        Set<String> collectedDbs = conversion.getDatabases().keySet();
        for (String database : collectedDbs) {
            DBMirror dbMirror = conversion.getDatabase(database);
            Set<String> tables = dbMirror.getTableMirrors().keySet();
            for (String table : tables) {
                TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
                GetTableMetadata tmd = new GetTableMetadata(config, dbMirror, tblMirror);
                gtf.add(config.getTransferThreadPool().schedule(tmd, 1, TimeUnit.MILLISECONDS));
            }
        }

        while (true) {
            boolean check = true;
            for (Future<ReturnStatus> sf : gtf) {
                if (!sf.isDone()) {
                    check = false;
                    break;
                }
                try {
                    if (sf.isDone() && sf.get() != null) {
                        if (sf.get().getStatus() == ReturnStatus.Status.ERROR) {
                            throw new RuntimeException(sf.get().getException());
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            if (check)
                break;
        }
        gtf.clear(); // reset

        LOG.info("==============================");
        LOG.info(conversion.toString());
        LOG.info("==============================");
        Date endTime = new Date();
        DecimalFormat df = new DecimalFormat("#.###");
        df.setRoundingMode(RoundingMode.CEILING);
        LOG.info("GATHERING METADATA: Completed in " + df.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");
        return rtn;

    }

}
