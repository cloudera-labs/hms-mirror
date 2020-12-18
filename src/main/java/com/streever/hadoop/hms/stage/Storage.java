package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.mirror.*;
import com.streever.hadoop.hms.util.TableUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Date;

public class Storage implements Runnable {
    private static Logger LOG = LogManager.getLogger(Storage.class);

    private Config config = null;
    private DBMirror dbMirror = null;
    private TableMirror tblMirror = null;
    private boolean successful = Boolean.FALSE;

    public boolean isSuccessful() {
        return successful;
    }

    public Storage(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        this.config = config;
        this.dbMirror = dbMirror;
        this.tblMirror = tblMirror;
    }

    public static Boolean fixConfig(Config config) {
        Boolean rtn = Boolean.TRUE;
        if (config.getStorage().getStrategy() == Strategy.SQL ||
                config.getStorage().getStrategy() == Strategy.HYBRID) {
            // Ensure the Partitions are built right away.
            if (!config.getCluster(Environment.UPPER).getPartitionDiscovery().getInitMSCK()) {
                config.getCluster(Environment.UPPER).getPartitionDiscovery().setInitMSCK(Boolean.TRUE);
                LOG.warn("Config Property OVERRIDE: PartitionDiscovery:InitMSCK set to true to support SQL STORAGE Mapping");
            }
        }
        if (config.getStorage().getStrategy() == Strategy.EXPORT_IMPORT ||
                config.getStorage().getStrategy() == Strategy.HYBRID) {
            // The hcfsNamespace for each cluster needs to be set.
            // We use it to build the correct relative location in the UPPER cluster
            //   from the LOWER cluster location.
            if (config.getCluster(Environment.LOWER).getHcfsNamespace() == null ||
                    config.getCluster(Environment.UPPER).getHcfsNamespace() == null) {
                LOG.error("The LOWER and UPPER cluster 'hcfsNamespace' values must be set to support " +
                        "the EXPORT_IMPORT migration strategy.");
                rtn = Boolean.FALSE;
            }
        }
        if ((config.getStorage().getStrategy() == Strategy.SQL ||
                config.getStorage().getStrategy() == Strategy.DISTCP) &&
                config.getStorage().getMigrateACID()) {
            LOG.error("Migrating ACID tables is only supported via EXPORT_IMPORT storage strategy.");
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    @Override
    public void run() {
        Date start = new Date();
        LOG.info("STORAGE: Migrating " + dbMirror.getDatabase() + "." + tblMirror.getName());

        // Set Database to Transfer DB.

        switch (config.getStorage().getStrategy()) {
            case SQL:
                if (!TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LOWER))) {
                    successful = doSQL();
                } else {
                    // ACID Table support ONLY available via EXPORT_IMPORT.
                    String errmsg = "ACID Table STORAGE support only available via EXPORT/IMPORT";
                    tblMirror.addIssue(errmsg);
                    LOG.debug(dbMirror.getDatabase() + "." + tblMirror.getName() + ":" +
                            "ACID Table STORAGE support only available via EXPORT/IMPORT");
                }
                break;
            case EXPORT_IMPORT:
                // Convert NON-ACID tables.
                    successful = doExportImport();
                break;
            case HYBRID:
                // Check the Size of the volume where the data is stored on the lower cluster.
                // TODO: Num files
                // TODO: Volume
                // DONE: ACID Tables
                // DONE: Num of Partitions sqlPartitionLimit
                if (TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LOWER)) ||
                        (TableUtils.isPartitioned(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LOWER)) &&
                                tblMirror.getPartitionDefinition(Environment.LOWER).size() >
                                        config.getStorage().getHybrid().getSqlPartitionLimit())) {
                    successful = doExportImport();
                } else {
                    successful = doSQL();
                }

                break;
            case DISTCP:
                break;
            default:
                throw new RuntimeException("Strategy: "+ config.getStorage().getStrategy().toString() + " isn't valid for the STORAGE phase.");
        }

        tblMirror.setPhaseSuccess(successful);

        Date end = new Date();
        Long diff = end.getTime() - start.getTime();
        tblMirror.setStageDuration(diff);
        LOG.info("STORAGE: Migration complete for " + dbMirror.getDatabase() + "." + tblMirror.getName() + " in " +
                Long.toString(diff) + "ms");
    }

    protected Boolean doSQL() {
        Boolean rtn = Boolean.FALSE;

        tblMirror.setStrategy(Strategy.SQL);
        tblMirror.incPhase();

        tblMirror.addAction("STORAGE", "via SQL");
        // Create table in new cluster.
        // Associate data to original cluster data.
        rtn = config.getCluster(Environment.UPPER).buildUpperSchemaUsingLowerData(config, dbMirror, tblMirror);

        // Build the Transfer table (with prefix)
        // NOTE: This will change the UPPER tabledef, so run this AFTER the buildUpperSchemaUsingLowerData
        // process
        if (rtn) {
            rtn = config.getCluster(Environment.UPPER).buildUpperTransferTable(config, dbMirror, tblMirror);
        }

        // Force the MSCK to enable the SQL transfer
        if (rtn) {
            rtn = config.getCluster(Environment.UPPER).partitionMaintenance(config, dbMirror, tblMirror, Boolean.TRUE);
        }
        // sql to move data.
        if (rtn) {
            rtn = config.getCluster(Environment.UPPER).doSqlDataTransfer(config, dbMirror, tblMirror);
        }

        // rename move table to target table.
        // TODO: Swap tables.
        if (rtn) {
            rtn = config.getCluster(Environment.UPPER).completeSqlTransfer(config, dbMirror, tblMirror);
        }

        return rtn;
    }

    protected Boolean doExportImport() {
        Boolean rtn = Boolean.FALSE;

        tblMirror.setStrategy(Strategy.EXPORT_IMPORT);
        tblMirror.incPhase();

        tblMirror.addAction("STORAGE", "EXPORT/IMPORT");
        // Convert NON-ACID tables.
        // Convert ACID tables WHEN 'migrateACID' is set.
        if (!TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LOWER)) ||
                (TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LOWER)) &&
                        config.getStorage().getMigrateACID())) {

            rtn = config.getCluster(Environment.LOWER).exportSchema(config, dbMirror.getDatabase(), dbMirror, tblMirror);
            if (rtn) {
                rtn = config.getCluster(Environment.UPPER).importSchemaWithData(config, dbMirror, tblMirror);
            }
        } else {
            String issuemsg = "ACID table migration only support when storage:migrateACID=true";
            tblMirror.addIssue(issuemsg);
        }

        return rtn;
    }
}
