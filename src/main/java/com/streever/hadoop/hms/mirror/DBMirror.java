package com.streever.hadoop.hms.mirror;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;

public class DBMirror {
    private static Logger LOG = LogManager.getLogger(DBMirror.class);

    private String name;

    private Map<String, TableMirror> tableMirrors = new TreeMap<String, TableMirror>();

    public DBMirror() {
    }

    public DBMirror(String name) {
        this.name = name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public TableMirror addTable(String table) {
        if (tableMirrors.containsKey(table)) {
            LOG.debug("Table object found in map: " + table);
            return tableMirrors.get(table);
        } else {
            LOG.debug("Adding table object to map: " + table);
            TableMirror tableMirror = new TableMirror(this.getName(), table);
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

}
