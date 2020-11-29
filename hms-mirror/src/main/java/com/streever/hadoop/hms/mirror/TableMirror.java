package com.streever.hadoop.hms.mirror;

import com.streever.hadoop.hms.util.TableUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TableMirror {
    private DBMirror database;
    private String name;

    public String getName() {
        return name;
    }

    public TableMirror(DBMirror database, String tablename) {
        this.database = database;
        this.name = tablename;
    }


    // There are two environments (UPPER and LOWER)
    private Map<Environment, List<String>> tableDefinitions = new TreeMap<Environment, List<String>>();
    private Map<Environment, Boolean> tablePartitioned = new TreeMap<Environment, Boolean>();
    private Map<Environment, List<String>> tablePartitions = new TreeMap<Environment, List<String>>();

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
     */
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

}
