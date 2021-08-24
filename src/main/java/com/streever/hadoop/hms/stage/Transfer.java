package com.streever.hadoop.hms.stage;

import com.streever.hadoop.HadoopSession;
import com.streever.hadoop.hms.mirror.*;
import com.streever.hadoop.hms.util.TableUtils;
import com.streever.hadoop.shell.command.CommandReturn;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Transfer implements Callable<ReturnStatus> {
    private static Logger LOG = LogManager.getLogger(Transfer.class);
    public static Pattern protocolNSPattern = Pattern.compile("(^.*://)([a-zA-Z0-9](?:(?:[a-zA-Z0-9-]*|(?<!-)\\.(?![-.]))*[a-zA-Z0-9]+)?)(:\\d{4})?");
    // Pattern to find the value of the last directory in a url.
    public static Pattern lastDirPattern = Pattern.compile(".*/([^/?]+).*");

    private Config config = null;
    private DBMirror dbMirror = null;
    private TableMirror tblMirror = null;
    private boolean successful = Boolean.FALSE;

    public boolean isSuccessful() {
        return successful;
    }

    public Transfer(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        this.config = config;
        this.dbMirror = dbMirror;
        this.tblMirror = tblMirror;
    }


    @Override
    public ReturnStatus call() {
        ReturnStatus rtn = new ReturnStatus();

        try {
            Date start = new Date();
            LOG.info("Migrating " + dbMirror.getName() + "." + tblMirror.getName());

            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);

            // Set Database to Transfer DB.
            tblMirror.setPhaseState(PhaseState.STARTED);

            tblMirror.setStrategy(config.getDataStrategy());

            tblMirror.incPhase();
            tblMirror.addStep("TRANSFER", config.getDataStrategy().toString());

            switch (config.getDataStrategy()) {
                case DUMP:
                case SCHEMA_ONLY:
                case LINKED:
                case COMMON:
                case EXPORT_IMPORT:
                    successful = doBasic();
                    break;
//                case INTERMEDIATE:
//                    successful = doIntermediate();
//                    break;
                case SQL:
                    if (config.getTransfer().getIntermediateStorage() == null)
                        successful = doSQL();
                    else
                        successful = doIntermediateTransfer();
                    break;
                case HYBRID:
                    successful = doHybrid();
                    break;
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

    protected Boolean doSQL() {
        Boolean rtn = Boolean.FALSE;

        rtn = tblMirror.buildoutDefinitions(config, dbMirror);

        if (rtn)
            rtn = AVROCheck();

        if (rtn)
            rtn = tblMirror.buildoutSql(config, dbMirror);

        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);
        EnvironmentTable set = tblMirror.getEnvironmentTable(Environment.SHADOW);

        // Construct Transfer SQL
        if (let.getPartitioned()) {
            // Check that the partition count doesn't exceed the configuration limit.
            // Build Partition Elements.
            String partElement = TableUtils.getPartitionElements(let);
            String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS,
                    set.getName(), ret.getName(), partElement);
            String transferDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, let.getPartitions().size());
            ret.addSql(new Pair(transferDesc, transferSql));
            if (let.getPartitions().size() > config.getHybrid().getSqlPartitionLimit()) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                        "limit (hybrid->sqlPartitionLimit) of " + config.getHybrid().getSqlPartitionLimit() +
                        ".  This value is used to abort migrations that have a high potential for failure.  " +
                        "The migration will need to be done manually OR try increasing the limit.");
                rtn = Boolean.FALSE;
            }
        } else {
            // No Partitions
            String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER, set.getName(), ret.getName());
            ret.addSql(new Pair(TableUtils.LOAD_FROM_SHADOW_DESC, transferSql));
        }


        // Clean up shadow table.
        String dropShadowSql = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
        ret.getSql().add(new Pair(TableUtils.DROP_SHADOW_TABLE, dropShadowSql));

        // Execute the RIGHT sql if config.execute.
        if (rtn && config.isExecute()) {
            config.getCluster(Environment.RIGHT).runSql(tblMirror);
        }
        return rtn;
    }

    protected Boolean doIntermediateTransfer() {
        Boolean rtn = Boolean.FALSE;

        rtn = tblMirror.buildoutDefinitions(config, dbMirror);
        if (rtn)
            rtn = tblMirror.buildoutSql(config, dbMirror);

        if (rtn) {
            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
            EnvironmentTable tet = tblMirror.getEnvironmentTable(Environment.TRANSFER);
            EnvironmentTable set = tblMirror.getEnvironmentTable(Environment.SHADOW);
            EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);

            // Construct Transfer SQL
            if (config.getCluster(Environment.LEFT).getLegacyHive()) {
                // We need to ensure that 'tez' is the execution engine.
                let.addSql(new Pair(MirrorConf.TEZ_EXECUTION_DESC, MirrorConf.SET_TEZ_AS_EXECUTION_ENGINE));
            }
            // Need to see if the table has partitions.
            if (let.getPartitioned()) {
                // Check that the partition count doesn't exceed the configuration limit.
                // Build Partition Elements.
                String partElement = TableUtils.getPartitionElements(let);
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS,
                        let.getName(), tet.getName(), partElement);
                String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, let.getPartitions().size());
                let.addSql(new Pair(transferDesc, transferSql));
                if (TableUtils.isACID(let)) {
                    if (let.getPartitions().size() > config.getMigrateACID().getPartitionLimit()) {
                        // The partition limit has been exceeded.  The process will need to be done manually.
                        let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                                "limit (migrateACID->partitionLimit) of " + config.getMigrateACID().getPartitionLimit() +
                                ".  This value is used to abort migrations that have a high potential for failure.  " +
                                "The migration will need to be done manually OR try increasing the limit.");
                        rtn = Boolean.FALSE;
                    }
                } else {
                    if (let.getPartitions().size() > config.getHybrid().getSqlPartitionLimit()) {
                        // The partition limit has been exceeded.  The process will need to be done manually.
                        let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                                "limit (hybrid->sqlPartitionLimit) of " + config.getHybrid().getSqlPartitionLimit() +
                                ".  This value is used to abort migrations that have a high potential for failure.  " +
                                "The migration will need to be done manually OR try increasing the limit.");
                        rtn = Boolean.FALSE;
                    }
                }
            } else {
                // No Partitions
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER, let.getName(), tet.getName());
                let.addSql(new Pair(TableUtils.STAGE_TRANSFER_DESC, transferSql));
            }

            // Construct the Shadow Transfer SQL
            if (let.getPartitioned()) {
                // Build Partition Elements.
                String partElement = TableUtils.getPartitionElements(let);
                String shadowTransferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS,
                        set.getName(), ret.getName(), partElement);
                String transferDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, let.getPartitions().size());
                ret.addSql(new Pair(transferDesc, shadowTransferSql));
            } else {
                // No Partitions
                String shadowTransferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER, set.getName(), ret.getName());
                ret.addSql(new Pair(TableUtils.LOAD_FROM_SHADOW_DESC, shadowTransferSql));
            }
            tblMirror.addStep("Transfer/Shadow SQL", "Built");

            // Execute the LEFT sql if config.execute.
            if (rtn && config.isExecute()) {
                rtn = config.getCluster(Environment.LEFT).runSql(tblMirror);
            }

            // Execute the RIGHT sql if config.execute.
            if (rtn && config.isExecute()) {
                rtn = config.getCluster(Environment.RIGHT).runSql(tblMirror);
            }

            // Cleanup, POST creation and TRANSFERS.
            // LEFT TRANSFER table
            List<Pair> leftCleanup = new ArrayList<Pair>();

            String useLeftDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
            Pair leftUsePair = new Pair(TableUtils.USE_DESC, useLeftDb);
            leftCleanup.add(leftUsePair);
            let.addSql(leftUsePair);

            String dropTransfer = MessageFormat.format(MirrorConf.DROP_TABLE, tet.getName());
            Pair leftDropPair = new Pair(TableUtils.DROP_TRANSFER_TABLE, dropTransfer);
            leftCleanup.add(leftDropPair);
            let.addSql(leftDropPair);
            tblMirror.addStep("LEFT ACID Transfer/Shadow SQL Cleanup", "Built");

            if (rtn && config.isExecute()) {
                // Run the Cleanup Scripts
                config.getCluster(Environment.LEFT).runSql(leftCleanup, tblMirror, Environment.LEFT);
            }

            // RIGHT Shadow table
            List<Pair> rightCleanup = new ArrayList<Pair>();

            String useRightDb = MessageFormat.format(MirrorConf.USE, config.getResolvedDB(dbMirror.getName()));
            Pair rightUsePair = new Pair(TableUtils.USE_DESC, useRightDb);
            rightCleanup.add(rightUsePair);
            ret.addSql(rightUsePair);

            String rightDropShadow = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());

            Pair rightDropPair = new Pair(TableUtils.DROP_SHADOW_TABLE, rightDropShadow);

            rightCleanup.add(rightDropPair);
            ret.addSql(rightDropPair);
            tblMirror.addStep("RIGHT ACID Shadow SQL Cleanup", "Built");

            if (rtn && config.isExecute()) {
                // Run the Cleanup Scripts
                config.getCluster(Environment.RIGHT).runSql(rightCleanup, tblMirror, Environment.RIGHT);
            }
        }
        return rtn;
    }

    protected Boolean doHybrid() {
        Boolean rtn = Boolean.FALSE;

        // Need to look at table.  ACID tables go to doACID()
        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let.getName(), let.getDefinition())) {
            tblMirror.setStrategy(DataStrategy.ACID);
            if (config.getMigrateACID().isOn()) {
                rtn = doIntermediateTransfer();
            } else {
                let.addIssue(TableUtils.ACID_NOT_ON);
                rtn = Boolean.FALSE;
            }
        } else {
            if (let.getPartitioned()) {
                if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit()) {
                    // SQL
                    let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the EXPORT_IMPORT " +
                            "partition limit (hybrid->exportImportPartitionLimit) of " + config.getHybrid().getExportImportPartitionLimit() +
                            ".  Hence, the SQL method has been selected for the migration.");

                    tblMirror.setStrategy(DataStrategy.SQL);
                    if (config.getTransfer().getIntermediateStorage() == null) {
                        rtn = doSQL();
                    } else {
                        rtn = doIntermediateTransfer();
                    }
                } else {
                    // EXPORT
                    tblMirror.setStrategy(DataStrategy.EXPORT_IMPORT);
                    rtn = doBasic();
                }
            } else {
                // EXPORT
                tblMirror.setStrategy(DataStrategy.EXPORT_IMPORT);
                rtn = doBasic();
            }
        }
        return rtn;
    }

    protected Boolean doBasic() {
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let.getName(), let.getDefinition()) &&
                !(config.getDataStrategy() == DataStrategy.DUMP
                        || config.getDataStrategy() == DataStrategy.SCHEMA_ONLY)) {
            if (config.getDataStrategy() != DataStrategy.COMMON) {
                rtn = doHybrid();
            } else {
                rtn = Boolean.FALSE;
                tblMirror.addIssue(Environment.RIGHT,
                        "Can't transfer SCHEMA reference on COMMON storage for ACID tables.");
            }
        } else {
            rtn = tblMirror.buildoutDefinitions(config, dbMirror);

            if (rtn)
                rtn = AVROCheck();

            if (rtn)
                rtn = tblMirror.buildoutSql(config, dbMirror);

            // If EXPORT_IMPORT, need to run LEFT queries.
            if (rtn && tblMirror.getStrategy() == DataStrategy.EXPORT_IMPORT && config.isExecute()) {
                rtn = config.getCluster(Environment.LEFT).runSql(tblMirror);
            }

            // Execute the RIGHT sql if config.execute.
            if (rtn && config.isExecute()) {
                rtn = config.getCluster(Environment.RIGHT).runSql(tblMirror);
            }
        }
        return rtn;
    }

    protected Boolean AVROCheck() {
        Boolean rtn = Boolean.TRUE;
        Boolean relative = Boolean.FALSE;
        // Check for AVRO
        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);
        if (TableUtils.isAVROSchemaBased(let)) {
            String leftPath = TableUtils.getAVROSchemaPath(let);
            String rightPath = null;
            LOG.debug("Original AVRO Schema path: " + leftPath);
                /* Checks:
                - Is Path prefixed with a protocol?
                    - (Y) Does it match the LEFT's hcfsNamespace.
                        - (Y) Replace prefix with RIGHT 'hcfsNamespace' prefix.
                        - (N) Throw WARNING and set return to FALSE.  We don't recognize the prefix and
                                 can't guarantee that we can retrieve the file.
                    - (N) Leave it and copy the file to the same relative path on the RIGHT
                 */
            Matcher matcher = protocolNSPattern.matcher(leftPath);
            // ProtocolNS Found.
            String cpCmd = null;
            if (matcher.find()) {
                // Return the whole set of groups.
                String lns = matcher.group(0);

                // Does it match the "LEFT" hcfsNamespace.
                String leftNS = config.getCluster(Environment.LEFT).getHcfsNamespace();
                if (leftNS.endsWith("/")) {
                    leftNS = leftNS.substring(0, leftNS.length() - 1);
                }
                if (lns.startsWith(leftNS)) {
                    // They match, so replace with RIGHT hcfs namespace.
                    String newNS = config.getCluster(Environment.RIGHT).getHcfsNamespace();
                    if (newNS.endsWith("/")) {
                        newNS = newNS.substring(0, newNS.length() - 1);
                    }
                    rightPath = leftPath.replace(leftNS, newNS);
                    TableUtils.updateAVROSchemaLocation(ret, rightPath);
                } else {
                    // Protocol found doesn't match configured hcfs namespace for LEFT.
                    String warning = "AVRO Schema URL was NOT adjusted. Current (LEFT) path did NOT match the " +
                            "LEFT hcfsnamespace. " + leftPath + " is NOT in the " + config.getCluster(Environment.LEFT).getHcfsNamespace() +
                            ". Can't determine change, so we'll not do anything.";
                    ret.addIssue(warning);
                    ret.addIssue("Schema creation may fail if location isn't available to RIGHT cluster.");
                    LOG.warn(warning);
                }
            } else {
                // No Protocol defined.  So we're assuming that its a relative path to the
                // defaultFS
                String rpath = "AVRO Schema URL appears to be relative: " + leftPath + ". No table definition adjustments.";
                ret.addIssue(rpath);
                LOG.debug(rpath);
                rightPath = leftPath;
                relative = Boolean.TRUE;
            }

            if (leftPath != null && rightPath != null && config.isCopyAvroSchemaUrls() && config.isExecute()) {
                // Copy over.
                HadoopSession session = null;
                try {
                    session = config.getCliPool().borrow();
                    CommandReturn cr = null;
                    if (relative) {
                        leftPath = config.getCluster(Environment.LEFT).getHcfsNamespace() + leftPath;
                        rightPath = config.getCluster(Environment.RIGHT).getHcfsNamespace() + rightPath;
                    }
                    LOG.info("AVRO Schema COPY from: " + leftPath + " to " + rightPath);
                    // Ensure the path for the right exists.
                    matcher = lastDirPattern.matcher(rightPath);
                    if (matcher.find()) {
                        String pathEnd = matcher.group(1);
                        String mkdir = rightPath.substring(0, rightPath.length() - pathEnd.length());
                        cr = session.processInput("mkdir -p " + mkdir);
                        if (cr.isError()) {
                            ret.addIssue("Problem creating directory " + mkdir + ". " + cr.getError());
                            rtn = Boolean.FALSE;
                        } else {
                            cr = session.processInput("cp -f " + leftPath + " " + rightPath);
                            if (cr.isError()) {
                                ret.addIssue("Problem copying AVRO schema file from " + leftPath + " to " +
                                        mkdir + ".\n```" + cr.getError() + "```");
                                rtn = Boolean.FALSE;
                            }
                        }
                    }
                } catch (Throwable t) {
                    LOG.error(t);
                    ret.addIssue(t.getMessage());
                    rtn = Boolean.FALSE;
                } finally {
                    if (session != null)
                        config.getCliPool().returnSession(session);
                }
            }
            tblMirror.addStep("AVRO", "Checked");
        } else {
            // Not AVRO, so no action (passthrough)
            rtn = Boolean.TRUE;
        }

        return rtn;
    }

}
