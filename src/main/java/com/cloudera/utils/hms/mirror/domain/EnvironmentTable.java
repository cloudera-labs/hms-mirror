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

package com.cloudera.utils.hms.mirror.domain;

import com.cloudera.utils.hms.mirror.CreateStrategy;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.util.TableUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static com.cloudera.utils.hms.mirror.MessageCode.TABLE_ISSUE;
import static java.util.Objects.nonNull;

@Getter
@Setter
@Slf4j
@JsonIgnoreProperties(ignoreUnknown = true)
public class EnvironmentTable implements Cloneable {

    private List<Pair> sql = new ArrayList<>();
    private List<Pair> cleanUpSql = new ArrayList<>();
    private String name = null;
    private boolean exists = Boolean.FALSE;
    private CreateStrategy createStrategy = CreateStrategy.NOTHING;
    private List<String> definition = new ArrayList<>();
    private String owner = null;
    private Map<String, String> partitions = new HashMap<>();
//    private List<String> actions = new ArrayList<>();
    private Map<String, String> addProperties = new TreeMap<>();
    private Map<String, Object> statistics = new HashMap<>();
    private List<String> issues = new ArrayList<>();
    @JsonIgnore
    private TableMirror parent = null;

    public EnvironmentTable() {
    }

    public EnvironmentTable(TableMirror parent) {
        this.parent = parent;
    }

//    public void addAction(String action) {
//        getActions().add(action);
//    }

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
        if (nonNull(sql)) {
            Pair pair = new Pair(desc, sql);
            addSql(pair);
        }
    }

    @JsonIgnore
    public Boolean getPartitioned() {
        Boolean rtn = Boolean.FALSE;
        rtn = !partitions.isEmpty() ? Boolean.TRUE : Boolean.FALSE;
        if (!rtn) {
            // Check the definition incase the partitions are empty.
            rtn = TableUtils.isPartitioned(this);
        }
        return rtn;
    }

    @JsonIgnore
    public boolean isDefined() {
        if (nonNull(definition) && !definition.isEmpty()) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    @Override
    public EnvironmentTable clone() throws CloneNotSupportedException {
        EnvironmentTable clone = (EnvironmentTable) super.clone();
        // /Clean up/detach the lists.
        clone.setSql(new ArrayList<>());
        clone.setCleanUpSql(new ArrayList<>());
        clone.setAddProperties(new TreeMap<>());
//         We don't want these to be cloned.
        clone.setStatistics(new HashMap<>());
        clone.setIssues(new ArrayList<>());
        // detach the definition with new objects.
        clone.setDefinition(new ArrayList<>(definition));
        clone.setPartitions(new HashMap<>(partitions));

        return clone;
    }
}
