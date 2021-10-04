package com.streever.hadoop.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streever.hadoop.hms.mirror.feature.Feature;
import com.streever.hadoop.hms.mirror.feature.Features;
import com.streever.hadoop.hms.util.TableUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class TableMirror {
    private static Logger LOG = LogManager.getLogger(TableMirror.class);

    private String dbName;
    private String name;
    private Date start = new Date();
    /*
    Use to indicate the tblMirror should be removed from processing, post setup.
     */
    @JsonIgnore
    private boolean remove = Boolean.FALSE;
    @JsonIgnore
    private String removeReason = null;
    @JsonIgnore
    private UUID unique = UUID.randomUUID();

    private DateFormat tdf = new SimpleDateFormat("HH:mm:ss.SSS");
    @JsonIgnore
    private List<Marker> steps = new ArrayList<Marker>();

    private DataStrategy strategy = null;

    // An ordinal value that we'll increment at each phase of the process
    private int currentPhase = 0;
    // An ordinal value, assign when we start processing, that indicates how many phase there will be.
    private int totalPhaseCount = 0;

    // Caption to help identify the current phase of the effort.
    @JsonIgnore
    private String migrationStageMessage = null;

    private PhaseState phaseState = PhaseState.INIT;

    @JsonIgnore
    private Long stageDuration = 0l;

    private Map<Environment, EnvironmentTable> environments = new TreeMap<Environment, EnvironmentTable>();

    public String getName() {
        return name;
    }

    public String getName(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getName();
    }

    public UUID getUnique() {
        return unique;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public boolean isRemove() {
        return remove;
    }

    public void setRemove(boolean remove) {
        this.remove = remove;
    }

    public String getRemoveReason() {
        return removeReason;
    }

    public void setRemoveReason(String removeReason) {
        this.removeReason = removeReason;
    }

    public PhaseState getPhaseState() {
        return phaseState;
    }

    public void setPhaseState(PhaseState phaseState) {
        this.phaseState = phaseState;
    }

    public Long getStageDuration() {
        return stageDuration;
    }

    public void setStageDuration(Long stageDuration) {
        this.stageDuration = stageDuration;
    }

    public TableMirror(String dbName, String tablename) {
        this.dbName = dbName;
        this.name = tablename;
        addStep("init", null);
    }

    public DataStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(DataStrategy strategy) {
        this.strategy = strategy;
    }

    public void incPhase() {
        currentPhase += 1;
        if (currentPhase >= totalPhaseCount) {
            totalPhaseCount = currentPhase + 1;
        }
    }

    public Map<Environment, EnvironmentTable> getEnvironments() {
        return environments;
    }

    public String getProgressIndicator(int width, int scale) {
        StringBuilder sb = new StringBuilder();
        int progressLength = (width / scale) * currentPhase;
        sb.append("\u001B[32m");
        sb.append(StringUtils.rightPad("=", progressLength - 1, "="));
        sb.append("\u001B[33m");
        sb.append(StringUtils.rightPad("-", width - progressLength, "-"));
        sb.append("\u001B[0m|");
        return sb.toString();
    }

    public void processingDone() {
        totalPhaseCount = currentPhase;
        // Clear message
        setMigrationStageMessage(null);
    }

    public void nextPhase(String msg) {
        incPhase();
        setMigrationStageMessage(msg);
    }

    public String getMigrationStageMessage() {
        return migrationStageMessage;
    }

    public void setMigrationStageMessage(String migrationStageMessage) {
        this.migrationStageMessage = migrationStageMessage;
        incPhase();
    }

    public void addStep(String key, Object value) {
        Date now = new Date();
        Long elapsed = now.getTime() - start.getTime();
        start = now; // reset
        BigDecimal secs = new BigDecimal(elapsed).divide(new BigDecimal(1000));///1000
        DecimalFormat decf = new DecimalFormat("#,###.00");
        String secStr = decf.format(secs);
        steps.add(new Marker(secStr, key, value));
    }

    public List<Marker> getSteps() {
        return steps;
    }

    public EnvironmentTable getEnvironmentTable(Environment environment) {
        EnvironmentTable et = environments.get(environment);
        if (et == null) {
            et = new EnvironmentTable();
            environments.put(environment, et);
        }
        return et;
    }

    @JsonIgnore
    public boolean isThereAnIssue(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getIssues().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public boolean isThereSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getSql().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public boolean whereTherePropsAdded(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getAddProperties().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public List<String> getIssues(Environment environment) {
        return getEnvironmentTable(environment).getIssues();
    }

    public Map<String, String> getPropAdd(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getAddProperties();
    }

    public List<Pair> getSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getSql();
    }

    public void addIssue(Environment environment, String issue) {
        if (issue != null) {
            String scrubbedIssue = issue.replace("\n", "<br/>");
            getIssues(environment).add(scrubbedIssue);
        }
    }

    public Boolean hasIssues() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : environments.entrySet()) {
            if (entry.getValue().getIssues().size() > 0)
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Boolean hasActions() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : environments.entrySet()) {
            if (entry.getValue().getActions().size() > 0)
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Boolean hasAddedProperties() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : environments.entrySet()) {
            if (entry.getValue().getAddProperties().size() > 0)
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public void setName(String name) {
        this.name = name;
    }

    private Boolean buildoutDUMPDefinition(Config config, DBMirror dbMirror) {
//        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout DUMP Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        // Standardize the LEFT def.
        // Remove DB from CREATE
        TableUtils.stripDatabase(let.getName(), let.getDefinition());

        // If not legacy, remove location from ACID tables.
        if (!config.getCluster(Environment.LEFT).getLegacyHive() &&
                TableUtils.isACID(let.getName(), let.getDefinition())) {
            TableUtils.stripLocation(let.getName(), let.getDefinition());
        }
        return Boolean.TRUE;
    }

    private Boolean buildoutSCHEMA_ONLYDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SCHEMA_ONLY Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Swap out the namespace of the LEFT with the RIGHT.
        copySpec.setReplaceLocation(Boolean.TRUE);
        if (config.convertManaged())
            copySpec.setUpgrade(Boolean.TRUE);
        if (!config.isReadOnly() || !config.isSync()) {
            copySpec.setTakeOwnership(Boolean.TRUE);
        } else if (copySpec.getUpgrade()) {
            ret.addIssue("Ownership (PURGE Option) not set because of either: `sync` or `ro|read-only` was specified in the config.");
        }
        if (config.isReadOnly()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }

        if (config.isSync()) {
            // We assume that the 'definitions' are only there is the
            //     table exists.
            if (!let.getExists() && ret.getExists()) {
                // If left is empty and right is not, DROP RIGHT.
                ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                ret.setCreateStrategy(CreateStrategy.DROP);
            } else if (let.getExists() && !ret.getExists()) {
                // If left is defined and right is not, CREATE RIGHT.
                ret.addIssue("Schema missing, will be CREATED");
                ret.setCreateStrategy(CreateStrategy.CREATE);
            } else if (let.getExists() && ret.getExists()) {
                // If left and right, check schema change and replace if necessary.
                // Compare Schemas.
                if (schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                    ret.addIssue("Schema exists AND matches.  No Actions Necessary.");
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                } else {
                    if (TableUtils.isExternalPurge(ret)) {
                        ret.addIssue("Schema exists AND DOESN'T match.  But the 'RIGHT' table is has a PURGE option set. " +
                                "We can NOT safely replace the table without compromising the data. No action will be taken.");
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                        return Boolean.FALSE;
                    } else {
                        ret.addIssue("Schema exists AND DOESN'T match.  It will be REPLACED (DROPPED and RECREATED).");
                        ret.setCreateStrategy(CreateStrategy.REPLACE);
                    }
                }
            }
            copySpec.setTakeOwnership(Boolean.FALSE);
        } else {
            if (ret.getExists()) {
                if (TableUtils.isView(ret)) {
                    ret.addIssue("View exists already.  Will REPLACE.");
                    ret.setCreateStrategy(CreateStrategy.REPLACE);
                } else {
                    // Already exists, no action.
                    ret.addIssue("Schema exists already, no action.  If you wish to rebuild the schema, " +
                            "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                    return Boolean.FALSE;
                }
            } else {
                ret.addIssue("Schema will be created");
                ret.setCreateStrategy(CreateStrategy.CREATE);
            }
        }

        // For ACID tables, we need to remove the location.
        // Hive 3 doesn't allow this to be set via SQL Create.
        if (TableUtils.isACID(let)) {
            copySpec.setStripLocation(Boolean.TRUE);
        }

        // Rebuild Target from Source.
        if (!TableUtils.isACID(let) || (TableUtils.isACID(let) && config.getMigrateACID().isOn())) {
            rtn = buildTableSchema(copySpec);
        } else {
            let.addIssue(TableUtils.ACID_NOT_ON);
            ret.setCreateStrategy(CreateStrategy.NOTHING);
            rtn = Boolean.FALSE;
        }

        // If not legacy, remove location from ACID tables.
        if (rtn && !config.getCluster(Environment.LEFT).getLegacyHive() &&
                TableUtils.isACID(let.getName(), let.getDefinition())) {
            TableUtils.stripLocation(let.getName(), let.getDefinition());
        }
        return rtn;
    }

    private Boolean buildoutLINKEDDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout LINKED Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Can't LINK ACID tables.
        if (TableUtils.isHiveNative(let) && !TableUtils.isACID(let)) {
            // Swap out the namespace of the LEFT with the RIGHT.
            copySpec.setReplaceLocation(Boolean.FALSE);
            if (config.convertManaged())
                copySpec.setUpgrade(Boolean.TRUE);
            // LINKED doesn't own the data.
            copySpec.setTakeOwnership(Boolean.FALSE);

            if (config.isSync()) {
                // We assume that the 'definitions' are only there is the
                //     table exists.
                if (!let.getExists() && ret.getExists()) {
                    // If left is empty and right is not, DROP RIGHT.
                    ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                    ret.setCreateStrategy(CreateStrategy.DROP);
                } else if (let.getExists() && !ret.getExists()) {
                    // If left is defined and right is not, CREATE RIGHT.
                    ret.addIssue("Schema missing, will be CREATED");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                } else if (let.getExists() && ret.getExists()) {
                    // If left and right, check schema change and replace if necessary.
                    // Compare Schemas.
                    if (schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                        ret.addIssue("Schema exists AND matches.  No Actions Necessary.");
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                    } else {
                        if (TableUtils.isExternalPurge(ret)) {
                            ret.addIssue("Schema exists AND DOESN'T match.  But the 'RIGHT' table is has a PURGE option set. " +
                                    "We can NOT safely replace the table without compromising the data. No action will be taken.");
                            ret.setCreateStrategy(CreateStrategy.LEAVE);
                            return Boolean.FALSE;
                        } else {
                            ret.addIssue("Schema exists AND DOESN'T match.  It will be REPLACED (DROPPED and RECREATED).");
                            ret.setCreateStrategy(CreateStrategy.REPLACE);
                        }
                    }
                }
                copySpec.setTakeOwnership(Boolean.FALSE);
            } else {
                if (ret.getExists()) {
                    // Already exists, no action.
                    ret.addIssue("Schema exists already, no action. If you wish to rebuild the schema, " +
                            "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                } else {
                    ret.addIssue("Schema will be created");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                }
            }
            // Rebuild Target from Source.
            rtn = buildTableSchema(copySpec);
        } else {
            let.addIssue("Can't LINK ACID tables");
            ret.setCreateStrategy(CreateStrategy.NOTHING);
        }
        return rtn;
    }

    private Boolean buildoutCOMMONDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout COMMON Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Can't LINK ACID tables.
        if (TableUtils.isHiveNative(let) && !TableUtils.isACID(let)) {
            // Swap out the namespace of the LEFT with the RIGHT.
            copySpec.setReplaceLocation(Boolean.FALSE);
            if (config.convertManaged())
                copySpec.setUpgrade(Boolean.TRUE);
            // COMMON owns the data unless readonly specified.
            if (!config.isReadOnly())
                copySpec.setTakeOwnership(Boolean.TRUE);

            if (config.isSync()) {
                // We assume that the 'definitions' are only there is the
                //     table exists.
                if (!let.getExists() && ret.getExists()) {
                    // If left is empty and right is not, DROP RIGHT.
                    ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                    ret.setCreateStrategy(CreateStrategy.DROP);
                } else if (let.getExists() && !ret.getExists()) {
                    // If left is defined and right is not, CREATE RIGHT.
                    ret.addIssue("Schema missing, will be CREATED");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                } else if (let.getExists() && ret.getExists()) {
                    // If left and right, check schema change and replace if necessary.
                    // Compare Schemas.
                    if (schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                        ret.addIssue("Schema exists AND matches.  No Actions Necessary.");
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                    } else {
                        if (TableUtils.isExternalPurge(ret)) {
                            ret.addIssue("Schema exists AND DOESN'T match.  But the 'RIGHT' table is has a PURGE option set. " +
                                    "We can NOT safely replace the table without compromising the data. No action will be taken.");
                            ret.setCreateStrategy(CreateStrategy.LEAVE);
                            return Boolean.FALSE;
                        } else {
                            ret.addIssue("Schema exists AND DOESN'T match.  It will be REPLACED (DROPPED and RECREATED).");
                            ret.setCreateStrategy(CreateStrategy.REPLACE);
                        }
                    }
                }
                // With sync, don't own data.
                copySpec.setTakeOwnership(Boolean.FALSE);
            } else {
                if (ret.getExists()) {
                    // Already exists, no action.
                    ret.addIssue("Schema exists already, no action. If you wish to rebuild the schema, " +
                            "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                } else {
                    ret.addIssue("Schema will be created");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                }
            }
            // Rebuild Target from Source.
            rtn = buildTableSchema(copySpec);
        } else {
            let.addIssue("Can't use COMMON for ACID tables");
            ret.setCreateStrategy(CreateStrategy.NOTHING);
        }
        return rtn;
    }

    /*
    The SQL Strategy uses LINKED clusters and is only valid against Legacy Managed and EXTERNAL
    tables.  NO ACID tables.

    - We create the same schema in the 'target' cluster for the TARGET.
    - We need the create and LINKED a shadow table to the LOWER clusters data.

    TODO: buildoutSQLDefinition
     */
    private Boolean buildoutSQLDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SQL Definition");

        // Different transfer technique.  Staging location.
        if (config.getTransfer().getIntermediateStorage() != null) {
            return buildoutIntermediateDefinition(config, dbMirror);
        }

        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        EnvironmentTable set = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);
        set = getEnvironmentTable(Environment.SHADOW);

        if (TableUtils.isACID(let)) {
            let.addIssue("ACID table migration NOT support in this scenario.");
            return Boolean.FALSE;
        }

        if (config.isSync()) {
            let.addIssue("Sync NOT supported in the scenario.");
            return Boolean.FALSE;
        }

        // Create a 'shadow' table definition on right cluster pointing to the left data.
        copySpec = new CopySpec(config, Environment.LEFT, Environment.SHADOW);

        if (config.convertManaged())
            copySpec.setUpgrade(Boolean.TRUE);

        // Don't claim data.  It will be in the LEFT cluster, so the LEFT owns it.
        copySpec.setTakeOwnership(Boolean.FALSE);

        // Create table with alter name in RIGHT cluster.
        copySpec.setTableNamePrefix(config.getTransfer().getTransferPrefix());

        if (ret.getExists()) {
            // Already exists, no action.
            ret.addIssue("Schema exists already, no action.  If you wish to rebuild the schema, " +
                    "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
            ret.setCreateStrategy(CreateStrategy.LEAVE);
        } else {
            ret.addIssue("Schema will be created");
            ret.setCreateStrategy(CreateStrategy.CREATE);
        }

        // Build Shadow from Source.
        rtn = buildTableSchema(copySpec);

        // Create final table in right.
        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);

        // Swap out the namespace of the LEFT with the RIGHT.
        copySpec.setReplaceLocation(Boolean.TRUE);
        if (config.convertManaged())
            copySpec.setUpgrade(Boolean.TRUE);
        if (config.isReadOnly()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        } else {
            copySpec.setTakeOwnership(Boolean.TRUE);
        }

        if (ret.getExists()) {
            // Already exists, no action.
            ret.addIssue("Schema exists already, no action.  If you wish to rebuild the schema, " +
                    "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
            ret.setCreateStrategy(CreateStrategy.LEAVE);
        } else {
            ret.addIssue("Schema will be created");
            ret.setCreateStrategy(CreateStrategy.CREATE);
        }

        // Rebuild Target from Source.
        rtn = buildTableSchema(copySpec);

        return rtn;
    }

    /*
     */
    private Boolean buildoutEXPORT_IMPORTDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout EXPORT_IMPORT Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        if (!TableUtils.isHiveNative(let) || TableUtils.isACID(let)) {
            let.addIssue("Can't process ACID tables, VIEWs, or Non Native Hive Tables with this strategy.");
            return Boolean.FALSE;
        }

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Swap out the namespace of the LEFT with the RIGHT.
        copySpec.setReplaceLocation(Boolean.TRUE);
        if (config.convertManaged())
            copySpec.setUpgrade(Boolean.TRUE);
        if (!config.isReadOnly() || !config.isSync()) {
            copySpec.setTakeOwnership(Boolean.TRUE);
        }
        if (config.isReadOnly()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }

        if (ret.getExists()) {
            // Already exists, no action.
            ret.addIssue("Schema exists already, no action.  If you wish to rebuild the schema, " +
                    "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
            ret.setCreateStrategy(CreateStrategy.LEAVE);
        } else {
            ret.addIssue("Schema will be created");
            ret.setCreateStrategy(CreateStrategy.CREATE);
            rtn = Boolean.TRUE;
        }

        if (rtn)
            // Build Target from Source.
            rtn = buildTableSchema(copySpec);

        return rtn;
    }

    /*
     */
    private Boolean buildoutIntermediateDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout Intermediate Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
//        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        CopySpec rightSpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);

        if (ret.getExists()) {
            // Already exists, no action.
            ret.addIssue("Schema exists already.  Can't do ACID transfer if schema already exists. Drop it and " +
                    "try again.");
            ret.setCreateStrategy(CreateStrategy.NOTHING);
            return Boolean.FALSE;
        } else {
            ret.setCreateStrategy(CreateStrategy.CREATE);
        }

        if (!TableUtils.isACID(let)) {
            // Managed to EXTERNAL
            rightSpec.setUpgrade(Boolean.TRUE);
        }

        // Build Target from Source.
        rtn = buildTableSchema(rightSpec);

        // Build Transfer Spec.
        CopySpec transferSpec = new CopySpec(config, Environment.LEFT, Environment.TRANSFER);
        transferSpec.setMakeNonTransactional(Boolean.TRUE);
        transferSpec.setTableNamePrefix(config.getTransfer().getTransferPrefix());
//        if (!TableUtils.isACID(let)) {
        // Managed to EXTERNAL
        transferSpec.setUpgrade(Boolean.TRUE);
//        }

        String transferLoc = null;
        // When intermediate storage specified, use it.
        if (config.getTransfer().getIntermediateStorage() != null) {
            String isLoc = config.getTransfer().getIntermediateStorage();
            // Deal with extra '/'
            isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
            transferLoc = isLoc + "/" +
                    config.getTransfer().getTransferPrefix() + this.getUnique() + "_" + this.getName();
        } else {
            //  strip location
            transferSpec.setStripLocation(Boolean.TRUE);
            // build loc of transfer based on export base dir.
            transferLoc = config.getCluster(Environment.LEFT).getHcfsNamespace() +
                    config.getTransfer().getExportBaseDirPrefix() + dbMirror.getName() +
                    "/" + getName();
        }

        if (transferLoc != null)
            transferSpec.setLocation(transferLoc);

        if (rtn)
            // Build transfer table.
            rtn = buildTableSchema(transferSpec);

        // Build Shadow Spec
        CopySpec shadowSpec = new CopySpec(config, Environment.LEFT, Environment.SHADOW);
        shadowSpec.setUpgrade(Boolean.TRUE);
        shadowSpec.setTakeOwnership(Boolean.FALSE);
        shadowSpec.setTableNamePrefix(config.getTransfer().getTransferPrefix());
        if (transferLoc != null)
            shadowSpec.setLocation(transferLoc);
        if (rtn)
            rtn = buildTableSchema(shadowSpec);

        return rtn;
    }


    /*
    TODO: buildoutHYBRIDDefinition
     */
    private Boolean buildoutHYBRIDDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout HYBRID Definition");
        EnvironmentTable let = null;

        let = getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let.getName(), let.getDefinition())) {
            if (config.getMigrateACID().isOn()) {
                rtn = buildoutIntermediateDefinition(config, dbMirror);
            } else {
                let.addIssue(TableUtils.ACID_NOT_ON);
            }
        } else {
            if (let.getPartitioned()) {
                if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit()) {
                    rtn = buildoutSQLDefinition(config, dbMirror);
                } else {
                    rtn = buildoutEXPORT_IMPORTDefinition(config, dbMirror);
                }

            }
        }

        return rtn;
    }

    public Boolean buildoutDefinitions(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        switch (getStrategy()) {
            case DUMP:
                rtn = buildoutDUMPDefinition(config, dbMirror);
                break;
            case SCHEMA_ONLY:
                rtn = buildoutSCHEMA_ONLYDefinition(config, dbMirror);
                break;
            case LINKED:
                rtn = buildoutLINKEDDefinition(config, dbMirror);
                break;
            case SQL:
                rtn = buildoutSQLDefinition(config, dbMirror);
                break;
            case EXPORT_IMPORT:
                rtn = buildoutEXPORT_IMPORTDefinition(config, dbMirror);
                break;
            case HYBRID:
                rtn = buildoutHYBRIDDefinition(config, dbMirror);
                break;
            case ACID:
                if (config.getMigrateACID().isOn()) {
                    rtn = buildoutIntermediateDefinition(config, dbMirror);
                } else {

                }
                break;
            case COMMON:
                rtn = buildoutCOMMONDefinition(config, dbMirror);
                break;
        }
        this.addStep("Definitions", "Built");
        return rtn;
    }

    private Boolean buildoutDUMPSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout DUMP SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        let.getSql().clear();
        useDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
        let.addSql(TableUtils.USE_DESC, useDb);

        createTbl = this.getCreateStatement(Environment.LEFT);
        let.addSql(TableUtils.CREATE_DESC, createTbl);

        // If partitioned, !ACID, repair
        if (let.getPartitioned() && !TableUtils.isACID(let) &&
                config.getCluster(Environment.LEFT).getPartitionDiscovery().getInitMSCK()) {
            String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, dbMirror.getName(), let.getName());
            let.addSql(TableUtils.REPAIR_DESC, msckStmt);
        }

        rtn = Boolean.TRUE;

        return rtn;

    }

    private Boolean buildoutSCHEMA_ONLYSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SCHEMA_ONLY SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        ret.getSql().clear();
        database = config.getResolvedDB(dbMirror.getName());
        useDb = MessageFormat.format(MirrorConf.USE, database);

        switch (ret.getCreateStrategy()) {
            case NOTHING:
            case LEAVE:
                // Do Nothing.
                break;
            case DROP:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);
                break;
            case REPLACE:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String dropStmt2 = null;
                if (TableUtils.isView(ret)) {
                    dropStmt2 = MessageFormat.format(MirrorConf.DROP_VIEW, ret.getName());
                } else {
                    dropStmt2 = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                }
                ret.addSql(TableUtils.DROP_DESC, dropStmt2);
                String createStmt = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt);
                break;
            case CREATE:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String createStmt2 = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                break;
        }

        // If partitioned, !ACID, repair
        if (let.getPartitioned() && !TableUtils.isACID(let) &&
                config.getCluster(Environment.RIGHT).getPartitionDiscovery().getInitMSCK() &&
                (ret.getCreateStrategy() == CreateStrategy.REPLACE || ret.getCreateStrategy() == CreateStrategy.CREATE)) {
            String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, ret.getName());
            ret.addSql(TableUtils.REPAIR_DESC, msckStmt);
        }

        rtn = Boolean.TRUE;
        return rtn;
    }

    /*
    TODO: buildoutSQLSql
     */
    private Boolean buildoutSQLSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SQL SQL");

        if (config.getTransfer().getIntermediateStorage() != null) {
            return buildoutIntermediateSql(config, dbMirror);
        }

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);
        EnvironmentTable set = getEnvironmentTable(Environment.SHADOW);

        ret.getSql().clear();

        // RIGHT SHADOW Table
        database = config.getResolvedDB(dbMirror.getName());
        useDb = MessageFormat.format(MirrorConf.USE, database);

        ret.addSql(TableUtils.USE_DESC, useDb);
        // Drop any previous SHADOW table, if it exists.
        String dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
        ret.addSql(TableUtils.DROP_DESC, dropStmt);

        // Create Shadow Table
        String shadowCreateStmt = getCreateStatement(Environment.SHADOW);
        ret.addSql(TableUtils.CREATE_SHADOW_DESC, shadowCreateStmt);
        // Repair Partitions
        if (let.getPartitioned()) {
            String shadowMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, set.getName());
            ret.addSql(TableUtils.REPAIR_DESC, shadowMSCKStmt);
        }

        // RIGHT Final Table
        switch (ret.getCreateStrategy()) {
            case NOTHING:
            case LEAVE:
                // Do Nothing.
                break;
            case DROP:
                dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);
                break;
            case REPLACE:
                dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);
                String createStmt = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt);
                break;
            case CREATE:
                String createStmt2 = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                break;
        }

        rtn = Boolean.TRUE;
        return rtn;
    }

    private Boolean buildoutIntermediateSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout Intermediate SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable tet = getEnvironmentTable(Environment.TRANSFER);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);
        EnvironmentTable set = getEnvironmentTable(Environment.SHADOW);

        ret.getSql().clear();

        // LEFT Transfer Table
        database = dbMirror.getName();
        useDb = MessageFormat.format(MirrorConf.USE, database);

        let.addSql(TableUtils.USE_DESC, useDb);
        // Drop any previous SHADOW table, if it exists.
        String transferDropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, tet.getName());
        let.addSql(TableUtils.DROP_DESC, transferDropStmt);

        // Create Transfer Table
        String transferCreateStmt = getCreateStatement(Environment.TRANSFER);
        let.addSql(TableUtils.CREATE_TRANSFER_DESC, transferCreateStmt);


        // RIGHT SHADOW Table
        database = config.getResolvedDB(dbMirror.getName());
        useDb = MessageFormat.format(MirrorConf.USE, database);

        ret.addSql(TableUtils.USE_DESC, useDb);
        // Drop any previous SHADOW table, if it exists.
        String dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
        ret.addSql(TableUtils.DROP_DESC, dropStmt);
        // Create Shadow Table
        String shadowCreateStmt = getCreateStatement(Environment.SHADOW);
        ret.addSql(TableUtils.CREATE_SHADOW_DESC, shadowCreateStmt);
        // Repair Partitions
        if (let.getPartitioned()) {
            String shadowMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, set.getName());
            ret.addSql(TableUtils.REPAIR_DESC, shadowMSCKStmt);
        }

        // RIGHT Final Table
        switch (ret.getCreateStrategy()) {
            case NOTHING:
            case LEAVE:
                // Do Nothing.
                break;
            case DROP:
                dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);
                break;
            case REPLACE:
                dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);
                String createStmt = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt);
                break;
            case CREATE:
                String createStmt2 = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                break;
        }

        rtn = Boolean.TRUE;
        return rtn;
    }

    /*
    TODO: buildoutEXPORT_IMPORTSql
     */
    private Boolean buildoutEXPORT_IMPORTSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout EXPORT_IMPORT SQL");

        String database = null;

        database = config.getResolvedDB(dbMirror.getName());

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        // LEFT Export to directory
        String useLeftDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
        let.addSql(TableUtils.USE_DESC, useLeftDb);
        String exportLoc = null;
        if (config.getTransfer().getIntermediateStorage() == null) {
            exportLoc = config.getTransfer().getExportBaseDirPrefix() + dbMirror.getName() + "/" + let.getName();
        } else {
            String isLoc = config.getTransfer().getIntermediateStorage();
            // Deal with extra '/'
            isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
            exportLoc = isLoc + "/" +
                    config.getTransfer().getTransferPrefix() + this.getUnique() + "_" + this.getName();
        }
        String exportSql = MessageFormat.format(MirrorConf.EXPORT_TABLE, let.getName(), exportLoc);
        let.addSql(TableUtils.EXPORT_TABLE, exportSql);

        // RIGHT IMPORT from Directory
        String useRightDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
        ret.addSql(TableUtils.USE_DESC, useRightDb);
        String importLoc = null;
        if (config.getTransfer().getIntermediateStorage() == null) {
            importLoc = config.getCluster(Environment.LEFT).getHcfsNamespace() + exportLoc;
        } else {
            importLoc = exportLoc;
        }
        String sourceLocation = TableUtils.getLocation(let.getName(), let.getDefinition());
        String targetLocation = config.getTranslator().translateTableLocation(database, let.getName(), sourceLocation, config);

        String importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, ret.getName(), importLoc, targetLocation);
        ret.addSql(TableUtils.IMPORT_TABLE, importSql);

        rtn = Boolean.TRUE;
        return rtn;
    }

    /*
    TODO: buildoutHYBRIDSql
     */
    private Boolean buildoutHYBRIDSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout HYBRID SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let.getName(), let.getDefinition())) {
            rtn = buildoutIntermediateSql(config, dbMirror);
        } else {
            if (let.getPartitioned()) {
                if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit()) {
                    rtn = buildoutSQLSql(config, dbMirror);
                } else {
                    rtn = buildoutEXPORT_IMPORTSql(config, dbMirror);
                }

            }
        }

        return rtn;
    }

    /*
    TODO: buildoutCOMMONSql
     */
    private Boolean buildoutCOMMONSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout COMMON SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        rtn = Boolean.TRUE;
        return rtn;
    }

    public Boolean buildoutSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);
        switch (getStrategy()) {
            case DUMP:
                rtn = buildoutDUMPSql(config, dbMirror);
                break;
            case COMMON:
            case SCHEMA_ONLY:
            case LINKED:
                rtn = buildoutSCHEMA_ONLYSql(config, dbMirror);
                break;
            case SQL:
                rtn = buildoutSQLSql(config, dbMirror);
                break;
            case EXPORT_IMPORT:
                rtn = buildoutEXPORT_IMPORTSql(config, dbMirror);
                break;
            case HYBRID:
                rtn = buildoutHYBRIDSql(config, dbMirror);
                break;
            case ACID:
                rtn = buildoutIntermediateSql(config, dbMirror);
                break;
//            case COMMON:
//                rtn = buildoutCOMMONSql(config, dbMirror);
//                break;
        }
        this.addStep("SQL", "Built");

        return rtn;
    }

    /*

     */
    public Boolean buildTableSchema(CopySpec copySpec) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Config config = copySpec.getConfig();

        EnvironmentTable source = getEnvironmentTable(copySpec.getSource());
        EnvironmentTable target = getEnvironmentTable(copySpec.getTarget());

        // Set Table Name
        if (source.getExists()) {
            target.setName(source.getName());

            // Clear the target spec.
            target.getDefinition().clear();
            // Reset with Source
            target.getDefinition().addAll(getTableDefinition(copySpec.getSource()));

            if (TableUtils.isHiveNative(source)) {
                // Rules
                // 1. Strip db from create state.  It's broken anyway with the way
                //      the quotes are.  And we're setting the target db in the context anyways.
                TableUtils.stripDatabase(target);

                if (copySpec.getLocation() != null)
                    TableUtils.updateTableLocation(target, copySpec.getLocation());

                // 1. If Managed, convert to EXTERNAL
                // When coming from legacy and going to non-legacy (Hive 3).
                Boolean converted = Boolean.FALSE;
                if (!TableUtils.isACID(source)) {
                    if (copySpec.getUpgrade() && TableUtils.isManaged(source)) {
                        converted = TableUtils.makeExternal(target);
                        if (converted) {
                            target.addIssue("Schema 'converted' from LEGACY managed to EXTERNAL");
                            target.addProperty(MirrorConf.HMS_MIRROR_LEGACY_MANAGED_FLAG, converted.toString());
                            target.addProperty(MirrorConf.HMS_MIRROR_CONVERTED_FLAG, converted.toString());
                            if (copySpec.getTakeOwnership()) {
                                target.addProperty(MirrorConf.EXTERNAL_TABLE_PURGE, "true");
                            } else {
                                target.addIssue("Ownership of the data not allowed in this scenario, PURGE flag NOT set.");
                            }
                        }
                    }
                } else {
                    // If target isn't legacy.
                    switch (copySpec.getTarget()) {
                        case SHADOW:
                            target.addProperty(MirrorConf.EXTERNAL_TABLE_PURGE, "false");
                            TableUtils.removeTblProperty(MirrorConf.TRANSACTIONAL, target);
                            converted = TableUtils.makeExternal(target);
                            break;
                        case TRANSFER:
                            if (config.getCluster(Environment.LEFT).getLegacyHive()) {
                                TableUtils.removeTblProperty(MirrorConf.TRANSACTIONAL, target);
                            } else {
                                target.addProperty(MirrorConf.EXTERNAL_TABLE_PURGE, "true");
                                TableUtils.removeTblProperty(MirrorConf.TRANSACTIONAL, target);
                                converted = TableUtils.makeExternal(target);
                            }
                            break;
                        case LEFT:
                            break;
                        case RIGHT:
                            if (!config.getCluster(copySpec.getTarget()).getLegacyHive()) {
                                target.addIssue("Location Stripped from ACID definition.  Location element in 'CREATE' " +
                                        "not allowed in Hive3+");
                                TableUtils.stripLocation(target);
                            }
                            break;
                    }

                    if (TableUtils.removeBuckets(target, config.getMigrateACID().getArtificialBucketThreshold())) {
                        target.addIssue("Bucket Definition removed (was " + TableUtils.numOfBuckets(source) + ") because it was EQUAL TO or BELOW " +
                                "the configured 'artificialBucketThreshold' of " +
                                config.getMigrateACID().getArtificialBucketThreshold());
                    }

                    if (copySpec.isMakeNonTransactional()) {
                        switch (copySpec.getTarget()) {
                            case LEFT:
                            case TRANSFER:
                                if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                                    TableUtils.makeExternal(target);
                                }
                                TableUtils.removeTblProperty(MirrorConf.TRANSACTIONAL, target);
                                break;
                            case SHADOW:
                            case RIGHT:
                                if (!config.getCluster(Environment.RIGHT).getLegacyHive()) {
                                    TableUtils.makeExternal(target);
                                }
                                TableUtils.removeTblProperty(MirrorConf.TRANSACTIONAL, target);
                        }

                    } else if (copySpec.isMakeExternal()) {
                        TableUtils.makeExternal(target);
                        TableUtils.removeTblProperty(MirrorConf.TRANSACTIONAL, target);
                    }

                }

                // 2. Set mirror stage one flag
                if (copySpec.getTarget() == Environment.RIGHT) {
                    target.addProperty(MirrorConf.HMS_MIRROR_METADATA_FLAG, df.format(new Date()));
                }

                // 3. Rename table
                if (copySpec.renameTable()) {
                    TableUtils.changeTableName(target, copySpec.getTableNamePrefix() + getName());
                }

                // 4. identify this table as being converted by hms-mirror
//        TableUtils.upsertTblProperty(MirrorConf.HMS_MIRROR_CONVERTED_FLAG, Boolean.TRUE.toString(), upperTD);

                // 5. Strip stat properties
                TableUtils.removeTblProperty("COLUMN_STATS_ACCURATE", target);
                TableUtils.removeTblProperty("numFiles", target);
                TableUtils.removeTblProperty("numRows", target);
                TableUtils.removeTblProperty("rawDataSize", target);
                TableUtils.removeTblProperty("totalSize", target);
                TableUtils.removeTblProperty("discover.partitions", target);

                // 6. Set 'discover.partitions' if config and non-acid
                if (config.getCluster(copySpec.getTarget()).getPartitionDiscovery().getAuto()) {
                    if (converted) {
                        target.addProperty(MirrorConf.DISCOVER_PARTITIONS, Boolean.TRUE.toString());
                    } else if (TableUtils.isExternal(target)) {
                        target.addProperty(MirrorConf.DISCOVER_PARTITIONS, Boolean.TRUE.toString());
                    }
                }

                // 5. Location Adjustments
                //    Since we are looking at the same data as the original, we're not changing this now.
                //    Any changes to data location are a part of stage-2 (STORAGE).
                switch (copySpec.getTarget()) {
                    case LEFT:
                    case RIGHT:
                        if (copySpec.getStripLocation()) {
                            TableUtils.stripLocation(target);
                        }

                        if (copySpec.getReplaceLocation() && !TableUtils.isACID(source)) {
                            String sourceLocation = TableUtils.getLocation(getName(), getTableDefinition(copySpec.getSource()));
                            String targetLocation = copySpec.getConfig().getTranslator().
                                    translateTableLocation(this.getDbName(), getName(), sourceLocation, copySpec.getConfig());
                            TableUtils.updateTableLocation(target, targetLocation);
                        }
                        break;
                    case SHADOW:
                    case TRANSFER:
                        if (copySpec.getLocation() != null) {
                            String isLoc = config.getTransfer().getIntermediateStorage();
                            // Deal with extra '/'
                            if (isLoc != null) {
                                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                                isLoc = isLoc + "/" +
                                        config.getTransfer().getTransferPrefix() + this.getUnique() + "_" + this.getName();
                                TableUtils.updateTableLocation(target, isLoc);
                            }
                        } else {
                            if (copySpec.getStripLocation()) {
                                TableUtils.stripLocation(target);
                            }

                            if (copySpec.getReplaceLocation() && !TableUtils.isACID(source)) {
                                String sourceLocation = TableUtils.getLocation(getName(), getTableDefinition(copySpec.getSource()));
                                String targetLocation = copySpec.getConfig().getTranslator().
                                        translateTableLocation(this.getDbName(), getName(), sourceLocation, copySpec.getConfig());
                                TableUtils.updateTableLocation(target, targetLocation);
                            }
                        }
                        break;
                }

                // 6. Go through the features, if any.
                if (!config.getSkipFeatures()) {
                    for (Features features : Features.values()) {
                        Feature feature = features.getFeature();
                        LOG.debug("Table: " + getName() + " - Checking Feature: " + features.toString());
                        if (feature.applicable(target)) {
                            LOG.debug("Table: " + getName() + " - Feature Applicable: " + features.toString());
                            target.addIssue("Feature (" + features.toString() + ") was found applicable and adjustments applied. " +
                                    feature.getDescription());
                            target = feature.fixSchema(target);
                        } else {
                            LOG.debug("Table: " + getName() + " - Feature NOT Applicable: " + features.toString());
                        }
                    }
                } else {
                    LOG.debug("Table: " + getName() + " - Skipping Features Check...");
                }

                // Add props to definition.
                if (whereTherePropsAdded(copySpec.getTarget())) {
                    Set<String> keys = target.getAddProperties().keySet();
                    for (String key : keys) {
                        TableUtils.upsertTblProperty(key, target.getAddProperties().get(key), target);
                    }
                }

            } else if (TableUtils.isView(target)) {
                source.addIssue("This is a VIEW.  It will be translated AS-IS.  View transitions will NOT honor " +
                        "target db name changes For example: `-dbp`.  VIEW creation depends on the referenced tables existing FIRST. " +
                        "VIEW creation failures may mean that all referenced tables don't exist yet.");
            } else {
                // This is a connector table.  IE: HBase, Kafka, JDBC, etc.  We just past it through.
                source.addIssue("This is not a NATIVE Hive table.  It will be translated 'AS-IS'.  If the libraries or dependencies required for this table definition are not available on the target cluster, the 'create' statement may fail.");
            }
        }
        return Boolean.TRUE;
    }

    public String getCreateStatement(Environment environment) {
        StringBuilder createStatement = new StringBuilder();
        List<String> tblDef = this.getTableDefinition(environment);
        if (tblDef != null) {
            Iterator<String> iter = tblDef.iterator();
            while (iter.hasNext()) {
                String line = iter.next();
                createStatement.append(line);
                if (iter.hasNext()) {
                    createStatement.append("\n");
                }
            }
        } else {
            throw new RuntimeException("Couldn't location definition for table: " + getName() +
                    " in environment: " + environment.toString());
        }
        return createStatement.toString();
    }

    public Boolean isPartitioned(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getPartitioned();
    }

    public List<String> getTableDefinition(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getDefinition();
    }

    public boolean schemasEqual(Environment one, Environment two) {
        List<String> schemaOne = getTableDefinition(one);
        List<String> schemaTwo = getTableDefinition(two);
        if (schemaOne != null && schemaTwo != null) {
            String fpOne = TableUtils.tableFieldsFingerPrint(schemaOne);
            String fpTwo = TableUtils.tableFieldsFingerPrint(schemaTwo);
            if (fpOne.equals(fpTwo)) {
                return Boolean.TRUE;
            } else {
                return Boolean.FALSE;
            }
        } else {
            return Boolean.FALSE;
        }
    }

    public void setTableDefinition(Environment environment, List<String> tableDefList) {
        EnvironmentTable et = getEnvironmentTable(environment);
        et.setDefinition(tableDefList);
    }

    public List<String> getPartitionDefinition(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getPartitions();
    }

    public void addTableAction(Environment environment, String action) {
        List<String> tableActions = getTableActions(environment);
        tableActions.add(action);
    }

    public List<String> getTableActions(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getActions();
    }

}
