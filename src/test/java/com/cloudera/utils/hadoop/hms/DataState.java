package com.cloudera.utils.hadoop.hms;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

public class DataState {

    private static DataState instance = null;

    protected String configuration = null;

//    protected Boolean dataCreated = Boolean.FALSE;
    protected Map<String, Boolean> dataCreated = new TreeMap<String, Boolean>();
    private Boolean skipAdditionDataCreation = Boolean.FALSE;

    protected Boolean execute = Boolean.FALSE;
    protected Boolean cleanUp = Boolean.TRUE;
    protected Boolean populate = Boolean.TRUE;

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

    public Boolean getSkipAdditionDataCreation() {
        return skipAdditionDataCreation;
    }

    public void setSkipAdditionDataCreation(Boolean skipAdditionDataCreation) {
        this.skipAdditionDataCreation = skipAdditionDataCreation;
    }

    public Boolean getPopulate() {
        return populate;
    }

    public void setPopulate(Boolean populate) {
        this.populate = populate;
    }

    public Boolean isDataCreated(String dataset) {
        Boolean rtn = Boolean.FALSE;
        if (!skipAdditionDataCreation) {
            if (dataCreated.containsKey(dataset))
                rtn = dataCreated.get(dataset);
        } else {
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Boolean isExecute() {
        return execute;
    }

    public void setCleanUp(Boolean cleanUp) {
        this.cleanUp = cleanUp;
    }

    public void setDataCreated(String dataset, Boolean dataCreated) {
        this.dataCreated.put(dataset, dataCreated);
//        this.dataCreated = dataCreated;
    }

    public void setExecute(Boolean execute) {
        this.execute = execute;
    }
}
