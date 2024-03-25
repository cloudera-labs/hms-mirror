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

import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hms.util.TableUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Getter
@Setter
public class TableMirror {
    @JsonIgnore
    private final List<Marker> steps = new ArrayList<Marker>();
    /*
    Use to indicate the tblMirror should be removed from processing, post setup.
     */
    @JsonIgnore
    private String unique = UUID.randomUUID().toString().replaceAll("-", "");
    private String name;
    @JsonIgnore
    private DBMirror parent;
    private Date start = new Date();
    @JsonIgnore
    private boolean remove = Boolean.FALSE;
    @JsonIgnore
    private String removeReason = null;
    private boolean reMapped = Boolean.FALSE;

    private DataStrategyEnum strategy = null;

    // An ordinal value that we'll increment at each phase of the process
    private AtomicInteger currentPhase = new AtomicInteger(0);
    // An ordinal value, assign when we start processing, that indicates how many phase there will be.
    private AtomicInteger totalPhaseCount = new AtomicInteger(0);

    // Caption to help identify the current phase of the effort.
    @JsonIgnore
    private String migrationStageMessage = null;

    private PhaseState phaseState = PhaseState.INIT;

    @JsonIgnore
    private Long stageDuration = 0L;

    private Map<Environment, EnvironmentTable> environments = null;

    public TableMirror() {
        addStep("init", null);
    }

    public void addIssue(Environment environment, String issue) {
        if (issue != null) {
            String scrubbedIssue = issue.replace("\n", "<br/>");
            getIssues(environment).add(scrubbedIssue);
        }
    }

    public void addStep(String key, Object value) {
        Date now = new Date();
        Long elapsed = now.getTime() - start.getTime();
        start = now; // reset
        BigDecimal secs = new BigDecimal(elapsed).divide(new BigDecimal(1000));///1000
        DecimalFormat decf = new DecimalFormat("#,###.00");
        String secStr = decf.format(secs);
        steps.add(new Marker(secStr, key, value));
    }

    public void addTableAction(Environment environment, String action) {
        List<String> tableActions = getTableActions(environment);
        tableActions.add(action);
    }

    public List<Pair> getCleanUpSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getCleanUpSql();
    }

    public EnvironmentTable getEnvironmentTable(Environment environment) {
        EnvironmentTable et = getEnvironments().get(environment);
        if (et == null) {
            et = new EnvironmentTable(this);
            getEnvironments().put(environment, et);
        }
        return et;
    }

    public Map<Environment, EnvironmentTable> getEnvironments() {
        if (environments == null) {
            environments = new TreeMap<Environment, EnvironmentTable>();
        }
        return environments;
    }

    public void setEnvironments(Map<Environment, EnvironmentTable> environments) {
        this.environments = environments;
        // Need to connect after deserialization.
        for (EnvironmentTable environmentTable : environments.values()) {
            environmentTable.setParent(this);
        }
    }

    public List<String> getIssues(Environment environment) {
        return getEnvironmentTable(environment).getIssues();
    }

    public String getName(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getName();
    }

    public Map<String, String> getPartitionDefinition(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getPartitions();
    }

    public String getProgressIndicator(int width) {
        StringBuilder sb = new StringBuilder();
        int progressLength = Math.floorDiv(Math.multiplyExact(width, currentPhase.get()), totalPhaseCount.get());
        log.info(this.getParent().getName() + ":" + this.getName() + " CurrentPhase: " + currentPhase.get() +
                " -> TotalPhaseCount: " + totalPhaseCount.get());
        log.info(this.getParent().getName() + ":" + this.getName() + " Progress: " + progressLength + " of " + width);
        sb.append("\u001B[32m");
        sb.append(StringUtils.rightPad("=", progressLength - 1, "="));
        sb.append("\u001B[33m");
        sb.append(StringUtils.rightPad("-", width - progressLength, "-"));
        sb.append("\u001B[0m|");
        return sb.toString();
    }

    public Map<String, String> getPropAdd(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getAddProperties();
    }

    public List<Pair> getSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getSql();
    }

    public List<String> getTableActions(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getActions();
    }

    public List<String> getTableDefinition(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getDefinition();
    }

    public boolean hasActions() {
        boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : getEnvironments().entrySet()) {
            if (!entry.getValue().getActions().isEmpty()) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    public boolean hasAddedProperties() {
        boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : getEnvironments().entrySet()) {
            if (!entry.getValue().getAddProperties().isEmpty()) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    public boolean hasIssues() {
        boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : getEnvironments().entrySet()) {
            if (!entry.getValue().getIssues().isEmpty()) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    public Boolean hasStatistics() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : getEnvironments().entrySet()) {
            if (entry.getValue().getStatistics().size() > 0) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    public void incPhase() {
        currentPhase.getAndIncrement();
        if (currentPhase.get() >= totalPhaseCount.get()) {
            totalPhaseCount.set(currentPhase.get() + 1);
        }
    }

    public void incTotalPhaseCount() {
        totalPhaseCount.getAndIncrement();
    }

    public Boolean isPartitioned(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getPartitioned();
    }

    @JsonIgnore
    public boolean isThereAnIssue(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getIssues().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public boolean isThereCleanupSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getCleanUpSql().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public boolean isThereSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getSql().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public void nextPhase(String msg) {
        incPhase();
        setMigrationStageMessage(msg);
    }

    public void processingDone() {
        totalPhaseCount = currentPhase;
        // Clear message
        setMigrationStageMessage(null);
    }

    public boolean schemasEqual(Environment one, Environment two) {
        List<String> schemaOne = getTableDefinition(one);
        List<String> schemaTwo = getTableDefinition(two);
        if (schemaOne != null && schemaTwo != null) {
            String fpOne = TableUtils.tableFieldsFingerPrint(schemaOne);
            String fpTwo = TableUtils.tableFieldsFingerPrint(schemaTwo);
            if (fpOne.equals(fpTwo)) {
                return Boolean.TRUE;
            } else {
                return Boolean.FALSE;
            }
        } else {
            return Boolean.FALSE;
        }
    }

    public void setMigrationStageMessage(String migrationStageMessage) {
        this.migrationStageMessage = migrationStageMessage;
        incPhase();
    }

    public void setTableDefinition(Environment environment, List<String> tableDefList) {
        EnvironmentTable et = getEnvironmentTable(environment);
        et.setDefinition(tableDefList);
    }

    public boolean whereTherePropsAdded(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return !et.getAddProperties().isEmpty() ? Boolean.TRUE : Boolean.FALSE;
    }

}
