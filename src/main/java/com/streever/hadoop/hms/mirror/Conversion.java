package com.streever.hadoop.hms.mirror;

import java.util.Map;
import java.util.TreeMap;

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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Conversion:").append("\n");
        sb.append("\tDatabases : ").append(databases.size()).append("\n");
        int tables = 0;
        int partitions = 0;
        for (String database: databases.keySet()) {
            DBMirror db = databases.get(database);
            tables += db.getTableMirrors().size();
            for (String table: db.getTableMirrors().keySet()) {
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
