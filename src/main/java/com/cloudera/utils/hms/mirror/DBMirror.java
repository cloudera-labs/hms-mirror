/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Getter
@Setter
@Slf4j
public class DBMirror {

    @JsonIgnore
    private final Map<Environment, List<String>> issues = new TreeMap<>();
    /*
    table - reason
     */
    private final Map<String, String> filteredOut = new TreeMap<>();
    @JsonIgnore
    private final Map<Environment, List<Pair>> sql = new TreeMap<>();
    private String name;
    @JsonIgnore
    private String resolvedName;
    private Map<Environment, Map<String, String>> dbDefinitions = new TreeMap<>();
    private Map<String, TableMirror> tableMirrors = null;

    /*
    Load a DBMirror instance from a yaml file using the Jackson YAML parser.
     */
    public static DBMirror load(String fileName) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        File dbMirrorFile = new File(fileName);
        DBMirror dbMirror = null;
        String yamlDBMirrorFile = null;
        try {
            yamlDBMirrorFile = IOUtils.toString(dbMirrorFile.toURI(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            dbMirror = mapper.readerFor(DBMirror.class).readValue(yamlDBMirrorFile);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return dbMirror;
    }

    public void addIssue(Environment environment, String issue) {
        String scrubbedIssue = issue.replace("\n", "<br/>");
        List<String> issuesList = issues.get(environment);
        if (issuesList == null) {
            issuesList = new ArrayList<>();
            issues.put(environment, issuesList);
        }
        issuesList.add(scrubbedIssue);
    }

    public TableMirror addTable(String table) {
        if (getTableMirrors().containsKey(table)) {
            log.debug("Table object found in map {}.{}", this.getName(), table);
            return getTableMirrors().get(table);
        } else {
            log.info("Adding table object to map {}.{}", this.getName(), table);
            TableMirror tableMirror = new TableMirror();
            tableMirror.setName(table);
            tableMirror.setParent(this);
            getTableMirrors().put(table, tableMirror);
            return tableMirror;
        }
    }

    public Map<String, String> getDBDefinition(Environment environment) {
        Map<String, String> rtn = dbDefinitions.get(environment);
        if (rtn == null) {
            rtn = new TreeMap<>();
            dbDefinitions.put(environment, rtn);
        }
        return rtn;
    }

    public Map<Environment, Map<String, String>> getDBDefinitions() {
        return dbDefinitions;
    }

    public void setDBDefinitions(Map<Environment, Map<String, String>> dbDefinitions) {
        this.dbDefinitions = dbDefinitions;
    }

    public Map<String, String> getFilteredOut() {
        return filteredOut;
    }

    public List<String> getIssuesList(Environment environment) {
        return issues.get(environment);
    }

    public Map<PhaseState, Integer> getPhaseSummary() {
        Map<PhaseState, Integer> rtn = new HashMap<>();
        for (String tableName : getTableMirrors().keySet()) {
            TableMirror tableMirror = getTableMirrors().get(tableName);
            Integer count = rtn.get(tableMirror.getPhaseState());
            if (count != null)
                rtn.put(tableMirror.getPhaseState(), count + 1);
            else
                rtn.put(tableMirror.getPhaseState(), 1);
        }
        return rtn;
    }

    @JsonIgnore
    public String getPhaseSummaryString() {
        StringBuilder sb = new StringBuilder();
        Map<PhaseState, Integer> psMap = getPhaseSummary();
        for (PhaseState ps : psMap.keySet()) {
            sb.append(ps).append("(").append(psMap.get(ps)).append(") ");
        }
        return sb.toString();
    }

    public List<Pair> getSql(Environment environment) {
        List<Pair> sqlList = null;
        if (sql.get(environment) == null) {
            sqlList = new ArrayList<>();
            sql.put(environment, sqlList);
        } else {
            sqlList = sql.get(environment);
        }
        return sqlList;
    }

    public TableMirror getTable(String table) {
        return getTableMirrors().get(table);
    }

    public Map<String, TableMirror> getTableMirrors() {
        if (tableMirrors == null) {
            tableMirrors = new TreeMap<>();
        }
        return tableMirrors;
    }

    public void setTableMirrors(Map<String, TableMirror> tableMirrors) {
        this.tableMirrors = tableMirrors;
        for (TableMirror tableMirror : tableMirrors.values()) {
            tableMirror.setParent(this);
        }
    }

    public boolean hasActions() {
        boolean rtn = Boolean.FALSE;
        for (Map.Entry<String, TableMirror> entry : getTableMirrors().entrySet()) {
            if (entry.getValue().hasActions())
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public boolean hasAddedProperties() {
        boolean rtn = Boolean.FALSE;
        for (Map.Entry<String, TableMirror> entry : getTableMirrors().entrySet()) {
            if (entry.getValue().hasAddedProperties())
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public boolean hasIssues() {
        boolean rtn = Boolean.FALSE;
        for (Map.Entry<String, TableMirror> entry : getTableMirrors().entrySet()) {
            if (entry.getValue().hasIssues())
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public boolean hasStatistics() {
        boolean rtn = Boolean.FALSE;
        for (Map.Entry<String, TableMirror> entry : getTableMirrors().entrySet()) {
            if (entry.getValue().hasStatistics())
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    @JsonIgnore
    public boolean isThereAnIssue() {
        return !issues.isEmpty() ? Boolean.TRUE : Boolean.FALSE;
    }

    public void setDBDefinition(Environment enviroment, Map<String, String> dbDefinition) {
        dbDefinitions.put(enviroment, dbDefinition);
    }

}
