package com.streever.hadoop.hms.stage;

import com.streever.hadoop.HadoopSession;
import com.streever.hadoop.hms.mirror.*;
import com.streever.hadoop.hms.util.TableUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;

public class Transfer implements Callable<ReturnStatus> {
    private static Logger LOG = LogManager.getLogger(Transfer.class);

    private Config config = null;
    private DBMirror dbMirror = null;
    private TableMirror tblMirror = null;
    private boolean successful = Boolean.FALSE;
    private HadoopSession cliSession;

    public boolean isSuccessful() {
        return successful;
    }

    public Transfer(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        this.config = config;
        this.dbMirror = dbMirror;
        this.tblMirror = tblMirror;
        this.cliSession = HadoopSession.get("Transfer: " + dbMirror.getName() + UUID.randomUUID());
    }


    @Override
    public ReturnStatus call() {
        ReturnStatus rtn = new ReturnStatus();

        try {
            Date start = new Date();
            LOG.info("Migrating " + dbMirror.getName() + "." + tblMirror.getName());

            // Set Database to Transfer DB.
            tblMirror.setPhaseState(PhaseState.STARTED);

            tblMirror.setStrategy(config.getDataStrategy());

            tblMirror.incPhase();
            tblMirror.addAction("TRANSFER", config.getDataStrategy().toString());

            if (TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT))) {
                if (config.getDataStrategy() == DataStrategy.EXPORT_IMPORT || config.getDataStrategy() == DataStrategy.HYBRID) {
                    switch (config.getDataStrategy()) {
                        case EXPORT_IMPORT:
                            successful = doExportImport();
                            break;
                        case HYBRID:
                            if (TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT)) ||
                                    (TableUtils.isPartitioned(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT)) &&
                                            tblMirror.getPartitionDefinition(Environment.LEFT).size() >
                                                    config.getHybrid().getSqlPartitionLimit())) {
                                successful = doExportImport();
                            } else {
                                successful = doSQL();
                            }
                            break;
                        default:
                            // Shouldn't end up here.
                            throw new RuntimeException("BAD flow.  Check ACID / data strategy logic.");
                    }
                } else {
                    successful = Boolean.FALSE;
                    tblMirror.addIssue("ACID tables only supported with EXPORT_IMPORT or HYBRID data strategies.");
                }
            } else {
                switch (config.getDataStrategy()) {
                    case SCHEMA_ONLY:
                        successful = (!TableUtils.isACID(tblMirror.getName(),
                                tblMirror.getTableDefinition(Environment.LEFT)) && doSchemaOnly());
                        break;
                    case LINKED:
                        successful = (!TableUtils.isACID(tblMirror.getName(),
                                tblMirror.getTableDefinition(Environment.LEFT)) && doLinked());
                        break;
                    case EXPORT_IMPORT:
                        successful = doExportImport();
                        break;
                    case INTERMEDIATE:
                        successful = (!TableUtils.isACID(tblMirror.getName(),
                                tblMirror.getTableDefinition(Environment.LEFT)) && doIntermediate());
                        break;
                    case COMMON:
                        successful = (!TableUtils.isACID(tblMirror.getName(),
                                tblMirror.getTableDefinition(Environment.LEFT)) && doCommon());
                        break;
                    case SQL:
                        successful = doSQL();
                        break;
                    case HYBRID:
                        if (TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT)) ||
                                (TableUtils.isPartitioned(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT)) &&
                                        tblMirror.getPartitionDefinition(Environment.LEFT).size() >
                                                config.getHybrid().getSqlPartitionLimit())) {
                            successful = doExportImport();
                        } else {
                            successful = doSQL();
                        }
                        break;
                }
            }

            if (successful)
                tblMirror.setPhaseState(PhaseState.SUCCESS);
            else
                tblMirror.setPhaseState(PhaseState.ERROR);


            Date end = new Date();
            Long diff = end.getTime() - start.getTime();
            tblMirror.setStageDuration(diff);
            LOG.info("Migration complete for " + dbMirror.getName() + "." + tblMirror.getName() + " in " +
                    Long.toString(diff) + "ms");
            rtn.setStatus(ReturnStatus.Status.SUCCESS);
        } catch (Throwable t) {
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(t);
        }
        return rtn;
    }

    protected Boolean doIntermediate() {
        return false;
    }

    protected Boolean doCommon() {
        return doLinked();
    }

    protected Boolean doSQL() {
        Boolean rtn = Boolean.FALSE;

        // Create table in new cluster.
        // Associate data to original cluster data.
        rtn = config.getCluster(Environment.RIGHT).buildUpperSchemaUsingLowerData(config, dbMirror, tblMirror);

        // Build the Transfer table (with prefix)
        // NOTE: This will change the RIGHT tabledef, so run this AFTER the buildUpperSchemaUsingLowerData
        // process
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).buildUpperTransferTable(config, dbMirror, tblMirror, config.getTransfer().getTransferPrefix());
        }

        // Force the MSCK to enable the SQL transfer
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).partitionMaintenance(config, dbMirror, tblMirror, Boolean.TRUE);
        }
        // sql to move data.
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).doSqlDataTransfer(config, dbMirror, tblMirror, config.getTransfer().getTransferPrefix());
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

        // Convert NON-ACID tables.
        // Convert ACID tables WHEN 'migrateACID' is set.
        if (!TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT)) ||
                (TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT)) &&
                        config.getMigrateACID())) {

            // TODO: Need to adjust this to first create the target table using the LEFT clusters
            //       data.  THEN, work on the IMPORT into the 'transfer' table.  Once that's complete
            //       do the swap of the tables.  Follow the SQL method in this regard.

            rtn = config.getCluster(Environment.LEFT).exportSchema(config, dbMirror.getName(), dbMirror, tblMirror, config.getTransfer().getExportBaseDirPrefix());
            if (rtn) {
                rtn = config.getCluster(Environment.RIGHT).importSchemaWithData(config, dbMirror, tblMirror, config.getTransfer().getExportBaseDirPrefix());
            }
        } else {
            String issuemsg = "ACID table migration only support when migrateACID=true";
            tblMirror.addIssue(issuemsg);
        }

        return rtn;
    }

    protected Boolean doLinked() {
        Boolean rtn = Boolean.FALSE;

        rtn = config.getCluster(Environment.RIGHT).buildUpperSchemaUsingLowerData(config, dbMirror, tblMirror);

//        if (rtn || (config.getReplicationStrategy() == ReplicationStrategy.SYNCHRONIZE && !rtn)) {
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).partitionMaintenance(config, dbMirror, tblMirror);
        }
        return rtn;
    }

    /*
    This set assumes the data has been migrated with DISTCP via some other process.  It will extract the schemas and
    recreate them on the target cluster, using the same 'relative' location for storage.
     */
    protected Boolean doSchemaOnly() {
        Boolean rtn = Boolean.FALSE;

        rtn = config.getCluster(Environment.RIGHT).buildUpperSchemaWithRelativeData(config, dbMirror, tblMirror);
//        if (rtn || (config.getReplicationStrategy() == ReplicationStrategy.SYNCHRONIZE && !rtn)) {
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).partitionMaintenance(config, dbMirror, tblMirror);
        }
        return rtn;
    }

}
