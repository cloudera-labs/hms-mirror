package com.cloudera.utils.hadoop.hms.mirror;

import java.util.*;

public class EnvironmentTable {

    private String name = null;
    private Boolean exists = Boolean.FALSE;
    private CreateStrategy createStrategy = CreateStrategy.NOTHING;
    private List<String> definition = new ArrayList<String>();
    private Boolean partitioned = Boolean.FALSE;
    private List<String> partitions = new ArrayList<String>();
    private List<String> actions = new ArrayList<String>();
    private Map<String, String> addProperties = new TreeMap<String, String>();
    private List<String> issues = new ArrayList<String>();
    private List<Pair> sql = new ArrayList<Pair>();

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

    public List<String> getDefinition() {
        return definition;
    }

    public void setDefinition(List<String> definition) {
        this.definition = definition;
    }

    public Boolean getPartitioned() {
        return partitions.size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<String> partitions) {
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
    }

    public void addSql(String desc, String sql) {
        Pair pair = new Pair(desc, sql);
        addSql(pair);
    }
}
