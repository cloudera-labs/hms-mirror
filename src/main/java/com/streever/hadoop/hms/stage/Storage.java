package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.mirror.*;
import com.streever.hadoop.hms.util.TableUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.concurrent.Callable;

public class Storage implements Callable<ReturnStatus> {
    private static Logger LOG = LogManager.getLogger(Storage.class);

    private Config config = null;
    private DBMirror dbMirror = null;
    private TableMirror tblMirror = null;
    private boolean successful = Boolean.FALSE;

    public boolean isSuccessful() {
        return successful;
    }

    // TODO: Handle DB level paths for DISTCP
    public Storage(Config config, DBMirror dbMirror) {
        this.config = config;
        this.dbMirror = dbMirror;
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
            if (!config.getCluster(Environment.RIGHT).getPartitionDiscovery().getInitMSCK()) {
                config.getCluster(Environment.RIGHT).getPartitionDiscovery().setInitMSCK(Boolean.TRUE);
                LOG.warn("Config Property OVERRIDE: PartitionDiscovery:InitMSCK set to true to support SQL STORAGE Mapping");
            }
        }
        if (config.getStorage().getStrategy() == Strategy.EXPORT_IMPORT ||
                config.getStorage().getStrategy() == Strategy.HYBRID) {
            // The hcfsNamespace for each cluster needs to be set.
            // We use it to build the correct relative location in the RIGHT cluster
            //   from the LEFT cluster location.
            if (config.getCluster(Environment.LEFT).getHcfsNamespace() == null ||
                    config.getCluster(Environment.RIGHT).getHcfsNamespace() == null) {
                LOG.error("The LEFT and RIGHT cluster 'hcfsNamespace' values must be set to support " +
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
    public ReturnStatus call() {
        ReturnStatus rtn = new ReturnStatus();
        try {
            Date start = new Date();
            LOG.info("STORAGE: Migrating " + dbMirror.getName() + "." + tblMirror.getName());

            tblMirror.setPhaseState(PhaseState.STARTED);

            // Set Database to Transfer DB.

            switch (config.getStorage().getStrategy()) {
                case SQL:
                    if (!TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT))) {
                        successful = doSQL();
                    } else {
                        // ACID Table support ONLY available via EXPORT_IMPORT.
                        String errmsg = "ACID Table STORAGE support only available via EXPORT/IMPORT";
                        tblMirror.addIssue(errmsg);
                        LOG.debug(dbMirror.getName() + "." + tblMirror.getName() + ":" +
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
                    if (TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT)) ||
                            (TableUtils.isPartitioned(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT)) &&
                                    tblMirror.getPartitionDefinition(Environment.LEFT).size() >
                                            config.getStorage().getHybrid().getSqlPartitionLimit())) {
                        successful = doExportImport();
                    } else {
                        successful = doSQL();
                    }

                    break;
                case DISTCP:
                    // TODO: HERE. PATH STRATEGY DB or TABLE. IF tblMirror is NULL or check the path strategy
                    //       in the config
                default:
                    successful = Boolean.FALSE;
                    tblMirror.addIssue(config.getStorage().getStrategy().toString() + " strategy NOT SUPPORTED in STORAGE Stage");
                    LOG.error("For " + dbMirror.getName() + ":" + tblMirror.getName() + " " +
                            config.getStorage().getStrategy() + " has not been implemented yet.");
                    break;
            }

            if (successful)
                tblMirror.setPhaseState(PhaseState.SUCCESS);
            else
                tblMirror.setPhaseState(PhaseState.ERROR);

            Date end = new Date();
            Long diff = end.getTime() - start.getTime();
            tblMirror.setStageDuration(diff);
            LOG.info("STORAGE: Migration complete for " + dbMirror.getName() + "." + tblMirror.getName() + " in " +
                    Long.toString(diff) + "ms");
            rtn.setStatus(ReturnStatus.Status.SUCCESS);
        } catch (Throwable t) {
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(t);
        }
        return rtn;
    }

    protected Boolean doSQL() {
        Boolean rtn = Boolean.FALSE;

        tblMirror.setStrategy(Strategy.SQL);
        tblMirror.incPhase();

        tblMirror.addAction("STORAGE", "via SQL");
        // Create table in new cluster.
        // Associate data to original cluster data.
        rtn = config.getCluster(Environment.RIGHT).buildUpperSchemaUsingLowerData(config, dbMirror, tblMirror);

        // Build the Transfer table (with prefix)
        // NOTE: This will change the RIGHT tabledef, so run this AFTER the buildUpperSchemaUsingLowerData
        // process
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).buildUpperTransferTable(config, dbMirror, tblMirror, config.getStorage().getTransferPrefix());
        }

        // Force the MSCK to enable the SQL transfer
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).partitionMaintenance(config, dbMirror, tblMirror, Boolean.TRUE);
        }
        // sql to move data.
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).doSqlDataTransfer(config, dbMirror, tblMirror, config.getStorage().getTransferPrefix());
        }

        // rename move table to target table.
        // TODO: Swap tables.
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).completeSqlTransfer(config, dbMirror, tblMirror);
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
        if (!TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT)) ||
                (TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT)) &&
                        config.getStorage().getMigrateACID())) {

            // TODO: Need to adjust this to first create the target table using the LEFT clusters
            //       data.  THEN, work on the IMPORT into the 'transfer' table.  Once that's complete
            //       do the swap of the tables.  Follow the SQL method in this regard.

            rtn = config.getCluster(Environment.LEFT).exportSchema(config, dbMirror.getName(), dbMirror, tblMirror, config.getStorage().getExportBaseDirPrefix());
            if (rtn) {
                rtn = config.getCluster(Environment.RIGHT).importSchemaWithData(config, dbMirror, tblMirror, config.getStorage().getExportBaseDirPrefix());
            }
        } else {
            String issuemsg = "ACID table migration only support when storage:migrateACID=true";
            tblMirror.addIssue(issuemsg);
        }

        return rtn;
    }
}
