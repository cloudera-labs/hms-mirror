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

import com.cloudera.utils.hms.util.TableUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

@Getter
@Setter
public class EnvironmentTable {

    private final List<Pair> sql = new ArrayList<Pair>();
    private final List<Pair> cleanUpSql = new ArrayList<Pair>();
    private String name = null;
    private boolean exists = Boolean.FALSE;
    private CreateStrategy createStrategy = CreateStrategy.NOTHING;
    private List<String> definition = new ArrayList<String>();
    private String owner = null;
    private Map<String, String> partitions = new HashMap<String, String>();
    private List<String> actions = new ArrayList<String>();
    private Map<String, String> addProperties = new TreeMap<String, String>();
    private Map<String, Object> statistics = new HashMap<String, Object>();
    private List<String> issues = new ArrayList<String>();
    @JsonIgnore
    private TableMirror parent = null;

    public EnvironmentTable() {
    }

    public EnvironmentTable(TableMirror parent) {
        this.parent = parent;
    }

    public void addAction(String action) {
        getActions().add(action);
    }

    public void addCleanUpSql(Pair sqlPair) {
        getCleanUpSql().add(sqlPair);
    }

    public void addCleanUpSql(String desc, String sql) {
        Pair pair = new Pair(desc, sql);
        addCleanUpSql(pair);
    }

    public void addIssue(String issue) {
        getIssues().add(issue);
    }

    public void addProperty(String key, String value) {
        getAddProperties().put(key, value);
    }

    public void addSql(Pair sqlPair) {
        getSql().add(sqlPair);
        parent.incTotalPhaseCount();
    }

    public void addSql(String desc, String sql) {
        Pair pair = new Pair(desc, sql);
        addSql(pair);
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

    @JsonIgnore
    public boolean isDefined() {
        if (definition != null && definition.size() > 0) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

}
