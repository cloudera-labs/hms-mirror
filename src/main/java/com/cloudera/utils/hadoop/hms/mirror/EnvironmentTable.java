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

import com.cloudera.utils.hadoop.hms.util.TableUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.*;

public class EnvironmentTable {

    private String name = null;
    private Boolean exists = Boolean.FALSE;
    private CreateStrategy createStrategy = CreateStrategy.NOTHING;
    private List<String> definition = new ArrayList<String>();
//    private final Boolean partitioned = Boolean.FALSE;
    private String owner = null;

    private Map<String, String> partitions = new HashMap<String, String>();
    private List<String> actions = new ArrayList<String>();
    private Map<String, String> addProperties = new TreeMap<String, String>();

    private Map<String, Object> statistics = new HashMap<String, Object>();

    private List<String> issues = new ArrayList<String>();
    private final List<Pair> sql = new ArrayList<Pair>();
    private final List<Pair> cleanUpsql = new ArrayList<Pair>();

    @JsonIgnore
    private TableMirror parent = null;

    public EnvironmentTable() {
    }

    public EnvironmentTable(TableMirror parent) {
        this.parent = parent;
    }

    public TableMirror getParent() {
        return parent;
    }

    public void setParent(TableMirror parent) {
        this.parent = parent;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getExists() {
        return exists;
    }

    public void setExists(Boolean exists) {
        this.exists = exists;
    }

    public CreateStrategy getCreateStrategy() {
        return createStrategy;
    }

    public void setCreateStrategy(CreateStrategy createStrategy) {
        this.createStrategy = createStrategy;
    }

    @JsonIgnore
    public Boolean isDefined() {
        if (definition != null && definition.size() > 0) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public List<String> getDefinition() {
        return definition;
    }

    public void setDefinition(List<String> definition) {
        this.definition = definition;
    }

    @JsonIgnore
    public Boolean getPartitioned() {
        Boolean rtn = Boolean.FALSE;
        rtn = partitions.size() > 0 ? Boolean.TRUE : Boolean.FALSE;
        if (!rtn) {
            // Check the definition incase the partitions are empty.
            rtn = TableUtils.isPartitioned(this);
        }
        return rtn;
    }

    public Map<String, String> getPartitions() {
        return partitions;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void setPartitions(Map<String, String> partitions) {
        this.partitions = partitions;
    }

    public List<String> getActions() {
        return actions;
    }

    public void setActions(List<String> actions) {
        this.actions = actions;
    }

    public void addAction(String action) {
        getActions().add(action);
    }

    public Map<String, String> getAddProperties() {
        return addProperties;
    }

    public void addProperty(String key, String value) {
        getAddProperties().put(key, value);
    }
    public void setAddProperties(Map<String, String> addProperties) {
        this.addProperties = addProperties;
    }

    public Map<String, Object> getStatistics() {
        return statistics;
    }

    public void setStatistics(Map<String, Object> statistics) {
        this.statistics = statistics;
    }

    public List<String> getIssues() {
        return issues;
    }

    public void addIssue(String issue) {
        getIssues().add(issue);
    }

    public void setIssues(List<String> issues) {
        this.issues = issues;
    }

    public List<Pair> getSql() {
        return sql;
    }

    public void addSql(Pair sqlPair) {
        getSql().add(sqlPair);
        parent.incTotalPhaseCount();
    }

    public void addSql(String desc, String sql) {
        Pair pair = new Pair(desc, sql);
        addSql(pair);
    }

    public List<Pair> getCleanUpSql() {
        return cleanUpsql;
    }

    public void addCleanUpSql(Pair sqlPair) {
        getCleanUpSql().add(sqlPair);
    }

    public void addCleanUpSql(String desc, String sql) {
        Pair pair = new Pair(desc, sql);
        addCleanUpSql(pair);
    }

}
