package com.cloudera.utils.hadoop.hms.mirror;

import java.util.Map;
import java.util.TreeMap;

public class EnvironmentMap {

    private String database;
    private Map<Environment, Map<String, String>> environmentMap = new TreeMap<>();

    public EnvironmentMap(String database) {
        this.database = database;
    }

    public synchronized Map<String, String> getLocationMap(Environment environment) {
        Map<String, String> rtn = environmentMap.get(environment);
        if (rtn == null) {
            rtn = new TreeMap<String, String>();
            environmentMap.put(environment, rtn);
        }
        return rtn;
    }
}
