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

    public DBMirror getDatabase(String database) {
        return databases.get(database);
    }

    public Integer translate(Cluster from, Cluster to, String database) {
        DBMirror dbMirror = databases.get(database);
        dbMirror.translate(from, to);


        return 0;
    }
}
