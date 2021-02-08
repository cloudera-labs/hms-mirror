package com.streever.hadoop.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streever.hadoop.hms.util.TableUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class TableMirror {
    private String dbName;
    private String name;
    private Date start = new Date();
    private DateFormat tdf = new SimpleDateFormat("HH:mm:ss.SSS");
    @JsonIgnore
    private Map<String[], Object> actions = new LinkedHashMap<String[], Object>();

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

    private List<String> issues = new ArrayList<String>();
    private List<String> propAdd = new ArrayList<String>();
    private List<String> sql = new ArrayList<String>();

    public String getName() {
        return name;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
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

    public TableMirror() {
    }

    public TableMirror(String dbName, String tablename) {
        this.dbName = dbName;
        this.name = tablename;
        addAction("init", null);
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

    public void addAction(String key, Object value) {
//        String tKey = tdf.format(new Date()) + " " + key;
        String[] keySet = new String[2];
        Date now = new Date();
        Long elapsed = now.getTime() - start.getTime();
        start = now; // reset
        BigDecimal secs = new BigDecimal(elapsed).divide(new BigDecimal(1000));///1000
        DecimalFormat decf = new DecimalFormat("#,###.00");
        String secStr = decf.format(secs);
        keySet[0] = secStr;
        keySet[1] = key;
//        String tKey = "[" + StringUtils.leftPad(secStr, 6, " ") +  "]" + key;
        actions.put(keySet, value);
    }

    public Map<String[], Object> getActions() {
        return actions;
    }

    @JsonIgnore
    public boolean isThereAnIssue() {
        return issues.size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public boolean isThereSql() {
        return sql.size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public boolean whereTherePropsAdded() {
        return propAdd.size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public List<String> getIssues() {
        return issues;
    }

    public List<String> getPropAdd() {
        return propAdd;
    }

    public List<String> getSql() {
        return sql;
    }

    public void addIssue(String issue) {
        String scrubbedIssue = issue.replace("\n", "<br/>");
        getIssues().add(scrubbedIssue);
    }

    public void addSql(String sqlOutput) {
        String scrubbedSql = sqlOutput.replace("\n", "<br/>");
        getSql().add(scrubbedSql);
    }

    public void addProp(String propAdd) {
        getPropAdd().add(propAdd);
    }

    public void addProp(String prop, String value) {
        getPropAdd().add(prop + "=" + value);
    }

    // There are two environments (RIGHT and LEFT)
    private Map<Environment, List<String>> tableDefinitions = new TreeMap<Environment, List<String>>();
    private Map<Environment, Boolean> tablePartitioned = new TreeMap<Environment, Boolean>();
    private Map<Environment, List<String>> tablePartitions = new TreeMap<Environment, List<String>>();
    private Map<Environment, List<String>> tableActions = new TreeMap<Environment, List<String>>();

    public void setName(String name) {
        this.name = name;
    }

    public void setPropAdd(List<String> propAdd) {
        this.propAdd = propAdd;
    }

    public Map<Environment, List<String>> getTableDefinitions() {
        return tableDefinitions;
    }

    public void setTableDefinitions(Map<Environment, List<String>> tableDefinitions) {
        this.tableDefinitions = tableDefinitions;
    }

    public Map<Environment, Boolean> getTablePartitioned() {
        return tablePartitioned;
    }

    public void setTablePartitioned(Map<Environment, Boolean> tablePartitioned) {
        this.tablePartitioned = tablePartitioned;
    }

    public Map<Environment, List<String>> getTablePartitions() {
        return tablePartitions;
    }

    public void setTablePartitions(Map<Environment, List<String>> tablePartitions) {
        this.tablePartitions = tablePartitions;
    }

    public boolean buildUpperSchema(Config config, Boolean takeOwnership) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<String> lowerTD = tableDefinitions.get(Environment.LEFT);
        List<String> upperTD = new ArrayList<String>();
        // Copy lower table definition to upper table def.
        upperTD.addAll(lowerTD);

        // Rules
        // 1. Strip db from create state.  It's broken anyway with the way
        //      the quotes are.  And we're setting the target db in the context anyways.
        TableUtils.stripDatabase(this.getName(), upperTD);

        // 1. If Managed, convert to EXTERNAL
        Boolean converted = TableUtils.makeExternal(this.getName(), upperTD);

        // 2. Set mirror stage one flag
//        TableUtils.upsertTblProperty(MirrorConf.HMS_MIRROR_METADATA_FLAG, df.format(new Date()), upperTD);

        // 3. setting legacy flag - At a later date, we'll convert this to
        //     'external.table.purge'='true'
        if (converted) {
            // If we want the new schema to take over the responsibility of the data. (purging)
            if (!takeOwnership) {
                TableUtils.upsertTblProperty(MirrorConf.HMS_MIRROR_LEGACY_MANAGED_FLAG, converted.toString(), upperTD);
                addProp(MirrorConf.HMS_MIRROR_LEGACY_MANAGED_FLAG, converted.toString());
                addIssue("Schema was Legacy Managed. The new schema does NOT have the 'external.table.purge' flag set because: You've requested it to be RO or it shares the underlying data.");
            } else {
                TableUtils.upsertTblProperty(MirrorConf.EXTERNAL_TABLE_PURGE, converted.toString(), upperTD);
                addProp(MirrorConf.EXTERNAL_TABLE_PURGE, converted.toString());
                // We need to add actions to the LEFT cluster to disable legacy managed behavior
                // When there is a shared dataset
                if (config.getDataStrategy() == DataStrategy.COMMON || config.getDataStrategy() == DataStrategy.LINKED) {
                    this.addTableAction(Environment.LEFT, "You Need to detach table ownership from filesystem in order to prevent accidental deletion of data that is now controlled by the RIGHT cluster.");
                }
            }
        }


        // 4. identify this table as being converted by hms-mirror
        TableUtils.upsertTblProperty(MirrorConf.HMS_MIRROR_CONVERTED_FLAG, Boolean.TRUE.toString(), upperTD);
        addProp(MirrorConf.HMS_MIRROR_CONVERTED_FLAG, converted.toString());

        // 5. Strip stat properties
        TableUtils.removeTblProperty("COLUMN_STATS_ACCURATE", upperTD);
        TableUtils.removeTblProperty("numFiles", upperTD);
        TableUtils.removeTblProperty("numRows", upperTD);
        TableUtils.removeTblProperty("rawDataSize", upperTD);
        TableUtils.removeTblProperty("totalSize", upperTD);
        TableUtils.removeTblProperty("discover.partitions", upperTD);

        // 6. Set 'discover.partitions' if config
        if (config.getCluster(Environment.RIGHT).getPartitionDiscovery().getAuto()) {
            TableUtils.upsertTblProperty(MirrorConf.DISCOVER_PARTITIONS, "true", upperTD);
            addProp(MirrorConf.DISCOVER_PARTITIONS, converted.toString());
        }

        // 5. Location Adjustments
        //    Since we are looking at the same data as the original, we're not changing this now.
        //    Any changes to data location are a part of stage-2 (STORAGE).

        this.setTableDefinition(Environment.RIGHT, upperTD);

        return Boolean.TRUE;
    }

    public String getCreateStatement(Environment environment, String prefix) {
        StringBuilder createStatement = new StringBuilder();
        List<String> tblDef = this.getTableDefinition(environment);
        if (tblDef != null) {

            List<String> tblDefCopy = new ArrayList<String>();
            tblDefCopy.addAll(tblDef);
            TableUtils.prefixTableName(this.getName(), prefix, tblDefCopy);

            Iterator<String> iter = tblDefCopy.iterator();
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

    // TODO: Partitions
    public void setPartitioned(Environment environment, Boolean partitioned) {
        tablePartitioned.put(environment, partitioned);
    }

    public Boolean isPartitioned(Environment environment) {
        Boolean rtn = tablePartitioned.get(environment);
        if (rtn != null) {
            return rtn;
        } else {
            return Boolean.FALSE;
        }
    }

    public List<String> getTableDefinition(Environment environment) {
        return tableDefinitions.get(environment);
    }

    public void setTableDefinition(Environment environment, List<String> tableDefList) {
        if (tableDefinitions.containsKey(environment)) {
            tableDefinitions.replace(environment, tableDefList);
        } else {
            tableDefinitions.put(environment, tableDefList);
        }
    }

    public List<String> getPartitionDefinition(Environment environment) {
        return tablePartitions.get(environment);
    }

    public void setPartitionDefinition(Environment environment, List<String> tablePartitionList) {
        if (tablePartitions.containsKey(environment)) {
            tablePartitions.replace(environment, tablePartitionList);
        } else {
            tablePartitions.put(environment, tablePartitionList);
        }
    }

    public void addTableAction(Environment environment, String action) {
        List<String> tableActions = getTableActions(environment);
        tableActions.add(action);
    }

    public List<String> getTableActions(Environment environment) {
        List<String> actions = tableActions.get(environment);
        if (actions == null) {
            actions = new ArrayList<String>();
            tableActions.put(environment, actions);
        }
        return actions;
    }

}
