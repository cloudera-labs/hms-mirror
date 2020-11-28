package com.streever.hadoop.hms.mirror;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DBMirror {
    private static Logger LOG = LogManager.getLogger(DBMirror.class);

    private String database;

    private Map<String, TableMirror> tableMirrors = new TreeMap<String, TableMirror>();

    public DBMirror(String database) {
        this.database = database;
    }

    public TableMirror addTable(String table) {
        if (tableMirrors.containsKey(table)) {
            LOG.debug("Table object found in map: " + table);
            return tableMirrors.get(table);
        } else {
            LOG.debug("Adding table object to map: " + table);
            TableMirror tableMirror = new TableMirror(this, table);
            tableMirrors.put(table, tableMirror);
            return tableMirror;
        }
    }

    public Map<String, TableMirror> getTableMirrors() {
        return tableMirrors;
    }

    public TableMirror getTable(String table) {
        return tableMirrors.get(table);
    }

    public Integer translate(Cluster from, Cluster to) {
        // loop through tables.
        Set<String> tables = tableMirrors.keySet();
        for (String table: tables) {
            TableMirror tableMirror = tableMirrors.get(table);
            tableMirror.translate(table, from, to);
        }
        return 0;
    }
}
