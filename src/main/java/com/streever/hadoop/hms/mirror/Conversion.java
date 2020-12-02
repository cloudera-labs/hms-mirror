package com.streever.hadoop.hms.mirror;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Conversion {
    private Map<String, DBMirror> databases = new TreeMap<String, DBMirror>();

    public DBMirror addDatabase(String database) {
        if (databases.containsKey(database)) {
            return databases.get(database);
        } else {
            DBMirror dbs = new DBMirror(database);
            databases.put(database, dbs);
            return dbs;
        }
    }

    public Map<String, DBMirror> getDatabases() {
        return databases;
    }

    public DBMirror getDatabase(String database) {
        return databases.get(database);
    }

    public Integer translate(Cluster from, Cluster to, String database) {
        DBMirror dbMirror = databases.get(database);
        dbMirror.translate(from, to);


        return 0;
    }

    public String toReport() {
        StringBuilder sb = new StringBuilder();
        Set<String> databaseSet = databases.keySet();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sb.append("# hms-mirror\n");
        sb.append(ReportingConf.substituteVariables("v.${Implementation-Version}")).append("\n");
        sb.append("Run Date: " + df.format(new Date())).append("\n\n");

        sb.append("**Legend**\n");
        sb.append("- ACID - Transactional/ACID Table").append("\n");
        sb.append("- PC - Partition Count").append("\n");
        sb.append("- TC - Transition Table Created").append("\n");
        sb.append("- EC - Export Created").append("\n");
        sb.append("- OW - Overwrite UPPER Cluster Table").append("\n");
        sb.append("- ETD - Existing Table Dropped").append("\n");
        sb.append("- IMPORTED - Upper Table Created").append("\n");
        sb.append("- DP - Discover Partitions(MSCK)").append("\n");
        sb.append("- LOC_ADJ - Location Adjusted to LOWER Cluster Storage").append("\n");
        sb.append("- PROP ADD - Properties Added").append("\n");
        sb.append("\n");


        for (String database : databaseSet) {
            sb.append("## ").append(database).append("\n");
            DBMirror dbMirror = databases.get(database);
            sb.append("\n");

            sb.append("|").append(" Table ").append("|")
                    .append(" ACID ").append("|")
                    .append(" PC ").append("|")
                    .append(" TC ").append("|")
                    .append(" EC ").append("|")
                    .append(" OW ").append("|")
                    .append(" ETD ").append("|")
                    .append(" IMPORTED ").append("|")
                    .append(" LOC_ADJ ").append("|")
                    .append(" DP ").append("|")
                    .append(" PROP ADD ").append("|")
                    .append(" Issues ").append("|")
                    .append("\n");
            sb.append("|").append(":---").append("|")
                    .append(":---:").append("|")
                    .append(":---:").append("|")
                    .append("---:").append("|")
                    .append(":---:").append("|")
                    .append(":---:").append("|")
                    .append(":---:").append("|")
                    .append(":---:").append("|")
                    .append(":---:").append("|")
                    .append(":---").append("|")
                    .append(":---").append("|")
                    .append("\n");
            Set<String> tables = dbMirror.getTableMirrors().keySet();
            for (String table : tables) {
                TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
                sb.append("|").append(table).append("|");
                sb.append(tblMirror.isTransactional() ? "X":" ").append("|");
                sb.append(tblMirror.getPartitionDefinition(Environment.LOWER) != null ?
                        tblMirror.getPartitionDefinition(Environment.LOWER).size() : " ").append("|");
                sb.append(tblMirror.isTransitionCreated() ? "X" : " ").append("|");
                sb.append(tblMirror.isExportCreated() ? "X" : " ").append("|")
                        .append(tblMirror.isOverwrite() ? "X" : " ").append("|")
                        .append(tblMirror.isExistingTableDropped() ? "X" : " ").append("|")
                        .append(tblMirror.isImported() ? "X" : " ").append("|")
                        .append(tblMirror.isLocationAdjusted() ? "X" : " ").append("|")
                        .append(tblMirror.isDiscoverPartitions() ? "X" : " ").append("|");
                if (tblMirror.whereTherePropsAdded()) {
                    for (String propAdd : tblMirror.getPropAdd()) {
                        sb.append(propAdd).append("<br/>");
                    }
                } else {
                    sb.append(" ");
                }
                sb.append("|");
                if (tblMirror.isThereAnIssue()) {
                    for (String issue : tblMirror.getIssues()) {
                        sb.append(issue).append("<br/>");
                    }
                } else {
                    sb.append(" ");
                }
                sb.append("|\n");
            }

        }
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Conversion:").append("\n");
        sb.append("\tDatabases : ").append(databases.size()).append("\n");
        int tables = 0;
        int partitions = 0;
        for (String database : databases.keySet()) {
            DBMirror db = databases.get(database);
            tables += db.getTableMirrors().size();
            for (String table : db.getTableMirrors().keySet()) {
                if (db.getTableMirrors().get(table).getPartitionDefinition(Environment.LOWER) != null) {
                    partitions += db.getTableMirrors().get(table).getPartitionDefinition(Environment.LOWER).size();
                }
            }
        }
        sb.append("\tTables    : " + tables).append("\n");
        sb.append("\tPartitions: " + partitions).append("\n");

        return sb.toString();
    }
}
