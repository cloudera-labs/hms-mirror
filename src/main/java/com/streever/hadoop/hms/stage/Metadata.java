package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.mirror.*;
import com.streever.hadoop.hms.util.TableUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Date;

public class Metadata implements Runnable {
    private static Logger LOG = LogManager.getLogger(Metadata.class);

    private Config config = null;
    private DBMirror dbMirror = null;
    private TableMirror tblMirror = null;
    private boolean successful = Boolean.FALSE;

    public boolean isSuccessful() {
        return successful;
    }

    public Metadata(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        this.config = config;
        this.dbMirror = dbMirror;
        this.tblMirror = tblMirror;
    }

    @Override
    public void run() {
        Date start = new Date();
        LOG.info("METADATA: Migrating " + dbMirror.getDatabase() + "." + tblMirror.getName());

        // Set Database to Transfer DB.

        switch (config.getMetadata().getStrategy()) {
            case DIRECT:
                // This method will skip the EXPORT transition step and build the schema from
                // the SOURCE table def.
                successful = doDIRECT();
                break;
            case TRANSITION:
                successful = doTRANSITION();
                break;
            default:
                throw new RuntimeException("Strategy: " + config.getStorage().getStrategy().toString() + " isn't valid for the METADATA phase.");
        }

        tblMirror.setPhaseSuccess(successful);

        Date end = new Date();
        Long diff = end.getTime() - start.getTime();
        tblMirror.setStageDuration(diff);
        LOG.info("METADATA: Migration complete for " + dbMirror.getDatabase() + "." + tblMirror.getName() + " in " +
                Long.toString(diff) + "ms");
    }

    protected Boolean doDIRECT() {
        Boolean rtn = Boolean.FALSE;

        tblMirror.setStrategy(Strategy.DIRECT);
        tblMirror.incPhase();

        tblMirror.addAction("METADATA", "DIRECT");

        rtn = config.getCluster(Environment.UPPER).buildUpperSchemaUsingLowerData(config, dbMirror, tblMirror);
        if (rtn) {
            rtn = config.getCluster(Environment.UPPER).partitionMaintenance(config, dbMirror, tblMirror);
        }
        return rtn;
    }

    protected Boolean doTRANSITION() {
        Boolean rtn = Boolean.FALSE;

        tblMirror.setStrategy(Strategy.TRANSITION);
        tblMirror.incPhase();

        tblMirror.addAction("METADATA", "TRANSITION");
        String transitionDatabase = config.getTransferPrefix() + dbMirror.getDatabase();
        // Method supporting a transfer Schema.  Had issues with this on EMR and the EXPORT process.
        // Could get the EXPORT the right permissions to write to S3, so the export would fail.
        rtn = config.getCluster(Environment.LOWER).buildTransferTableSchema(config, transitionDatabase, dbMirror, tblMirror);
        if (rtn) {
            rtn = config.getCluster(Environment.LOWER).exportSchema(config, transitionDatabase, dbMirror, tblMirror);
        }
        if (rtn) {
            rtn = config.getCluster(Environment.UPPER).importTransferSchemaUsingLowerData(config, dbMirror, tblMirror);
        }
        if (rtn) {
            rtn = config.getCluster(Environment.UPPER).partitionMaintenance(config, dbMirror, tblMirror);
        }

        return rtn;

    }

}
