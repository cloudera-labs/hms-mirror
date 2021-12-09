/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DBMirror {
    private static Logger LOG = LogManager.getLogger(DBMirror.class);

    private String name;
    private Map<Environment, Map<String, String>> dbDefinitions = new TreeMap<Environment, Map<String, String>>();
    private List<String> issues = new ArrayList<String>();
    /*
    table - reason
     */
    private Map<String, String> filteredOut = new TreeMap<String, String>();

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

    @JsonIgnore
    public boolean isThereAnIssue() {
        return issues.size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public void addIssue(String issue) {
        String scrubbedIssue = issue.replace("\n", "<br/>");
        getIssues().add(scrubbedIssue);
    }

    public List<String> getIssues() {
        return issues;
    }

    public Map<String, String> getFilteredOut() {
        return filteredOut;
    }

    public Map<Environment, Map<String, String>> getDBDefinitions() {
        return dbDefinitions;
    }

    public Map<String, String> getDBDefinition(Environment environment) {
        return dbDefinitions.get(environment);
    }

    public void setDBDefinition(Environment enviroment, Map<String, String> dbDefinition) {
        dbDefinitions.put(enviroment, dbDefinition);
    }

    public void setDBDefinitions(Map<Environment, Map<String, String>> dbDefinitions) {
        this.dbDefinitions = dbDefinitions;
    }

    /*
    Return String[3] for Hive.  0-Create Sql, 1-Location, 2-Mngd Location.
     */
    public String[] dbCreate(Config config) {
        String[] rtn = new String[3];
        StringBuilder sb = new StringBuilder();
        // Start with the LEFT definition.
        Map<String, String> dbDef = getDBDefinition(Environment.LEFT);
        String database = config.getResolvedDB(getName());
        String location = dbDef.get(MirrorConf.DB_LOCATION);
        String managedLocation = null;
        if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
            // Check for Managed Location.
            managedLocation = dbDef.get(MirrorConf.DB_MANAGED_LOCATION);
        }
//        if (config.getDataStrategy() != DataStrategy.DUMP) {
            String leftNamespace = config.getCluster(Environment.LEFT).getHcfsNamespace();
            String rightNamespace = config.getCluster(Environment.RIGHT).getHcfsNamespace();
//        }

        switch (config.getDataStrategy()) {
            case SCHEMA_ONLY:
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
                if (location != null) {
                    location = location.replace(leftNamespace, rightNamespace);
                    rtn[1] = location;
                }
                if (managedLocation != null) {
                    managedLocation = managedLocation.replace(leftNamespace, rightNamespace);
                    rtn[2] = managedLocation;
                }
                break;
            case DUMP:
            case COMMON:
            case LINKED:
                break;
        }
        sb.append("CREATE DATABASE IF NOT EXISTS ").append(database).append("\n");
        if (dbDef.get(MirrorConf.COMMENT) != null && dbDef.get(MirrorConf.COMMENT).trim().length() > 0) {
            sb.append(MirrorConf.COMMENT).append(" \"").append(dbDef.get(MirrorConf.COMMENT)).append("\"\n");
        }
        if (location != null) {
            sb.append(MirrorConf.DB_LOCATION).append(" \"").append(location).append("\"\n");
        }
        if (managedLocation != null) {
            sb.append(MirrorConf.DB_MANAGED_LOCATION).append(" \"").append(managedLocation).append("\"\n");
        }
        // TODO: DB Properties.
        rtn[0] = sb.toString();
        return rtn;
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

    public Boolean hasIssues() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<String, TableMirror> entry: tableMirrors.entrySet()) {
            if (entry.getValue().hasIssues())
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Boolean hasActions() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<String, TableMirror> entry: tableMirrors.entrySet()) {
            if (entry.getValue().hasActions())
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Boolean hasAddedProperties() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<String, TableMirror> entry: tableMirrors.entrySet()) {
            if (entry.getValue().hasAddedProperties())
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

}
