package com.cloudera.utils.hadoop.hms;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DataState {

    private static DataState instance = null;

    protected String configuration = null;

    protected Boolean dataCreated = Boolean.FALSE;
    protected Boolean execute = Boolean.FALSE;
    protected Boolean cleanUp = Boolean.TRUE;

    protected String working_db = null;
    protected String table_filter = null;

    private final String unique = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());

    private DataState() {

    }

    public static DataState getInstance() {
        if (instance == null)
            instance = new DataState();
        return instance;
    }

    public String getUnique() {
        return unique;
    }

    public String getConfiguration() {
        return configuration;
    }

    public void setConfiguration(String configuration) {
        this.configuration = System.getProperty("user.home") +
                "/.hms-mirror/cfg/" + configuration;
    }

    public String getTable_filter() {
        return table_filter;
    }

    public void setTable_filter(String table_filter) {
        this.table_filter = table_filter;
    }

    protected String getTestDbName() {
        return "z_hms_mirror_testdb_" + unique;
    }

    public String getWorking_db() {
        if (working_db == null)
            working_db = getTestDbName();
        return working_db;
    }

    public void setWorking_db(String working_db) {
        this.working_db = working_db;
    }

    public Boolean isCleanUp() {
        return cleanUp;
    }

    public Boolean isDataCreated() {
        return dataCreated;
    }

    public Boolean isExecute() {
        return execute;
    }

    public void setCleanUp(Boolean cleanUp) {
        this.cleanUp = cleanUp;
    }

    public void setDataCreated(Boolean dataCreated) {
        this.dataCreated = dataCreated;
    }

    public void setExecute(Boolean execute) {
        this.execute = execute;
    }
}
