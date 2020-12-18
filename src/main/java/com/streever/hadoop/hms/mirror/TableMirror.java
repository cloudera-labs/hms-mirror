package com.streever.hadoop.hms.mirror;

import com.streever.hadoop.hms.util.TableUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class TableMirror {
    private DBMirror database;
    private String name;
    private Map<String, Object> actions = new LinkedHashMap<String, Object>();

    private Stage stage = null;
    private Strategy strategy = null;

    // An ordinal value that we'll increment at each phase of the process
    private int currentPhase = 0;
    // An ordinal value, assign when we start processing, that indicates how many phase there will be.
    private int totalPhaseCount = 0;

    // Caption to help identify the current phase of the effort.
    private String migrationStageMessage = null;

    private Boolean phaseSuccess = Boolean.FALSE;
    private Long stageDuration = 0l;

    private List<String> issues = new ArrayList<String>();
    private List<String> propAdd = new ArrayList<String>();

    public String getName() {
        return name;
    }

    public Boolean getPhaseSuccess() {
        return phaseSuccess;
    }

    public void setPhaseSuccess(Boolean phaseSuccess) {
        this.phaseSuccess = phaseSuccess;
    }

    public Long getStageDuration() {
        return stageDuration;
    }

    public void setStageDuration(Long stageDuration) {
        this.stageDuration = stageDuration;
    }

    public TableMirror(DBMirror database, String tablename) {
        this.database = database;
        this.name = tablename;
    }

    public Stage getStage() {
        return stage;
    }

    public void setStage(Stage stage) {
        this.stage = stage;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    public void incPhase() {
        currentPhase =+ 1;
        if (currentPhase >= totalPhaseCount) {
            totalPhaseCount = currentPhase + 1;
        }
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
    }

    public void addAction(String key, Object value) {
        actions.put(key, value);
    }

    public Map<String, Object> getActions() {
        return actions;
    }

    public boolean isThereAnIssue() {
        return issues.size() > 0 ? Boolean.TRUE : Boolean.FALSE;
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

    public void addIssue(String issue) {
        String scrubbedIssue = issue.replace("\n", "<br/>");
        getIssues().add(scrubbedIssue);
    }

    public void addProp(String propAdd) {
        getPropAdd().add(propAdd);
    }
    public void addProp(String prop, String value) {
        getPropAdd().add(prop + "=" + value);
    }

    // There are two environments (UPPER and LOWER)
    private Map<Environment, List<String>> tableDefinitions = new TreeMap<Environment, List<String>>();
    private Map<Environment, Boolean> tablePartitioned = new TreeMap<Environment, Boolean>();
    private Map<Environment, List<String>> tablePartitions = new TreeMap<Environment, List<String>>();

    public boolean buildUpperSchema(Config config) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<String> lowerTD = tableDefinitions.get(Environment.LOWER);
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
            TableUtils.upsertTblProperty(MirrorConf.HMS_MIRROR_LEGACY_MANAGED_FLAG, converted.toString(), upperTD);
            addProp(MirrorConf.HMS_MIRROR_LEGACY_MANAGED_FLAG, converted.toString());
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
        if (config.getCluster(Environment.UPPER).getPartitionDiscovery().getAuto()) {
            TableUtils.upsertTblProperty(MirrorConf.DISCOVER_PARTITIONS, "true", upperTD);
            addProp(MirrorConf.DISCOVER_PARTITIONS, converted.toString());
        }

        // 5. Location Adjustments
        //    Since we are looking at the same data as the original, we're not changing this now.
        //    Any changes to data location are a part of stage-2 (STORAGE).

        this.setTableDefinition(Environment.UPPER, upperTD);

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

    private Boolean isPartitioned(Environment environment) {
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

    /*
    Build upper environment table def from lower environment definition.
    public void translate(String tableName, Cluster from, Cluster to) {
        List<String> tblDef = null;

        // Get Lower and convert to Upper
        List<String> fromTblDef = tableDefinitions.get(from);
        if (fromTblDef != null) {
            List<String> toTblDef = new ArrayList<String>(fromTblDef);
            tableDefinitions.put(to.getEnvironment(), toTblDef);
            if (TableUtils.isLegacyManaged(from, tableName, fromTblDef)) {

            }
//            TableUtils.updateTblProperty("numRows", "0", toTblDef);
            // CREATE TABLE to CREATE EXTERNAL TABLE
            // Add 'IF NOT EXISTS'?
            // When CREATE TABLE ...  add 'purge'?  For Stage 2, yes.  For Stage 1, add marker to tblproperties
            //    'legacy.managed'
            // Add tbl property. 'hive.mirror'='stage-1'
            // Change LOCATION from old Namespace to NEW namespace.
            // Remove 'COLUMN_STATS_ACCURATE' from tbl properties.
            TableUtils.removeTblProperty("COLUMN_STATS_ACCURATE", toTblDef);
            // Remove 'numFiles'
            TableUtils.removeTblProperty("numFiles", toTblDef);
            // Remove 'numRows'
            TableUtils.removeTblProperty("numRows", toTblDef);
            // Remove 'rawDataSize'
            TableUtils.removeTblProperty("rawDataSize", toTblDef);
            // Remove 'totalSize'
            TableUtils.removeTblProperty("totalSize", toTblDef);
            // Remove 'transient_lastDdlTime'
            TableUtils.removeTblProperty("transient_lastDdlTime", toTblDef);
            // If ORC, is there a rule to set bucket version or something...?

            int locIdx = toTblDef.indexOf("LOCATION");
            String location = toTblDef.get(locIdx + 1);
//            int tProps = toTblDef.indexOf("TBLPROPERTIES (");
//            toTblDef.add(tProps + 1, " 'external.table.purge'='true'");
            System.out.println(toTblDef.toString());
        } else {
            // No table def found.
        }

    }
     */

}
