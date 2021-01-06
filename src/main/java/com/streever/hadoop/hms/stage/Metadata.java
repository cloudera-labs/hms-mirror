package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.mirror.*;
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
        LOG.info("METADATA: Migrating " + dbMirror.getName() + "." + tblMirror.getName());

        // Set Database to Transfer DB.
        tblMirror.setPhaseState(PhaseState.STARTED);

        switch (config.getMetadata().getStrategy()) {
            case DIRECT:
                // This method will skip the EXPORT transition step and build the schema from
                // the SOURCE table def.
                successful = doDIRECT();
                break;
            case EXPORT_IMPORT:
                successful = doEXPORT_IMPORT();
                break;
            case DISTCP:
                successful = doDISTCP();
                break;
            case SCHEMA_EXTRACT:
                // TODO: Implement Schema Extract
            default:
                successful = Boolean.FALSE;
                tblMirror.addIssue(config.getMetadata().getStrategy().toString() + " strategy NOT SUPPORTED in METADATA Stage");
                LOG.error("For " + dbMirror.getName() + ":" + tblMirror.getName() + " " +
                        config.getMetadata().getStrategy() + " has not been implemented yet.");
                break;
        }

        if (successful)
            tblMirror.setPhaseState(PhaseState.SUCCESS);
        else
            tblMirror.setPhaseState(PhaseState.ERROR);

        Date end = new Date();
        Long diff = end.getTime() - start.getTime();
        tblMirror.setStageDuration(diff);
        LOG.info("METADATA: Migration complete for " + dbMirror.getName() + "." + tblMirror.getName() + " in " +
                Long.toString(diff) + "ms");
    }

    /*
    This set assumes the data has been migrated with DISTCP via some other process.  It will extract the schemas and
    recreate them on the target cluster, using the same 'relative' location for storage.
     */
    protected Boolean doDISTCP() {
        Boolean rtn = Boolean.FALSE;
        tblMirror.setStrategy(Strategy.DISTCP);
        tblMirror.incPhase();

        tblMirror.addAction("METADATA", Strategy.DISTCP.toString());

        rtn = config.getCluster(Environment.UPPER).buildUpperSchemaWithRelativeData(config, dbMirror, tblMirror);
        if (rtn) {
            rtn = config.getCluster(Environment.UPPER).partitionMaintenance(config, dbMirror, tblMirror);
        }
        return rtn;
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

    protected Boolean doEXPORT_IMPORT() {
        Boolean rtn = Boolean.FALSE;

        tblMirror.setStrategy(Strategy.EXPORT_IMPORT);
        tblMirror.incPhase();

        tblMirror.addAction("METADATA", Strategy.EXPORT_IMPORT.toString());
        String transitionDatabase = config.getMetadata().getTransferPrefix() + dbMirror.getName();
        // Method supporting a transfer Schema.  Had issues with this on EMR and the EXPORT process.
        // Could get the EXPORT the right permissions to write to S3, so the export would fail.
        rtn = config.getCluster(Environment.LOWER).buildTransferTableSchema(config, transitionDatabase, dbMirror, tblMirror);
        if (rtn) {
            rtn = config.getCluster(Environment.LOWER).exportSchema(config, transitionDatabase, dbMirror, tblMirror, config.getMetadata().getExportBaseDirPrefix());
        }
        if (rtn) {
            rtn = config.getCluster(Environment.UPPER).importTransferSchemaUsingLowerData(config, dbMirror, tblMirror, config.getMetadata().getExportBaseDirPrefix());
        }
        if (rtn) {
            rtn = config.getCluster(Environment.UPPER).partitionMaintenance(config, dbMirror, tblMirror);
        }

        return rtn;

    }

}
