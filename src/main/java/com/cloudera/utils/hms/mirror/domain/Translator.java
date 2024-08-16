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

import com.cloudera.utils.hms.mirror.EnvironmentMap;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.StringLengthComparator;
import com.cloudera.utils.hms.mirror.domain.support.TableType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Slf4j
@Getter
@Setter
@JsonIgnoreProperties(value = {"dbLocationMap"}, ignoreUnknown = true)
public class Translator implements Cloneable {
//    private int consolidationLevelBase = 1;
//    @Schema(description = "If the Partition Spec doesn't match the partition hierarchy, then set this to true.")
//    private boolean partitionLevelMismatch = Boolean.FALSE;

    @JsonIgnore
    private final Map<String, EnvironmentMap> translationMap = new TreeMap<>();

    /*
    Use this to force the location element in the external table create statements and
    not rely on the database 'location' element.
     */
    private boolean forceExternalLocation = Boolean.FALSE;
    /*
    GLM Built by the system from the Warehouse Plans.
     */
    private Map<String, Map<TableType, String>> autoGlobalLocationMap = null;
    /*
    GLM's that are manually added by the user.
     */
    private Map<String, Map<TableType, String>> userGlobalLocationMap = null;

    @JsonIgnore
    private Map<String, Map<TableType, String>> orderedGlobalLocationMap = null;

    private WarehouseMapBuilder warehouseMapBuilder = new WarehouseMapBuilder();

    /*
    After a session run where we capture state, we need to clean it up before running another session.
     */
    public void reset() {
        if (nonNull(translationMap))
            translationMap.clear();
//        if (nonNull(userGlobalLocationMap))
//            userGlobalLocationMap.clear();
        if (nonNull(autoGlobalLocationMap))
            autoGlobalLocationMap.clear();
        if (nonNull(orderedGlobalLocationMap))
            orderedGlobalLocationMap.clear();
        if (nonNull(warehouseMapBuilder))
            warehouseMapBuilder.reset();
    }

    public void addUserGlobalLocationMap(TableType tableType, String from, String to) {
        if (isNull(userGlobalLocationMap))
            userGlobalLocationMap = new HashMap<>();
        Map<TableType, String> target = userGlobalLocationMap.get(from);
        if (isNull(target))
            target = new HashMap<TableType, String>();
        target.put(tableType, to);
        userGlobalLocationMap.put(from, target);
//        rebuildOrderedGlobalLocationMap();
        getOrderedGlobalLocationMap().put(from, target);
    }

    // Needed to handle npe when loaded from json
//    public WarehouseMapBuilder getWarehouseMapBuilder() {
//        if (isNull(warehouseMapBuilder))
//            warehouseMapBuilder = new WarehouseMapBuilder();
//        return warehouseMapBuilder;
//    }

    @Override
    public Translator clone() {
        try {
            Translator clone = (Translator) super.clone();
            if (nonNull(userGlobalLocationMap))
                clone.userGlobalLocationMap = new HashMap<>(userGlobalLocationMap);
            if (nonNull(autoGlobalLocationMap))
                clone.autoGlobalLocationMap = new TreeMap<>(autoGlobalLocationMap);
            if (nonNull(warehouseMapBuilder))
                clone.warehouseMapBuilder = (WarehouseMapBuilder) warehouseMapBuilder.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    /*
    Remove the entry or sub entry if 'tableType' is not null.
     */
    public void removeUserGlobalLocationMap(String from, TableType tableType) {
        if (isNull(tableType))
            userGlobalLocationMap.remove(from);
        else {
            Map<TableType, String> target = userGlobalLocationMap.get(from);
            if (nonNull(target))
                target.remove(tableType);
        }
//        return getOrderedGlobalLocationMap().remove(from);
    }

//    public List<String> removeUserGlobalLocationMap(List<String> fromList) {
//        List<String> rtn = new ArrayList<>();
//        for (String from : fromList) {
//            rtn.add(userGlobalLocationMap.remove(from));
//            rtn.add(getOrderedGlobalLocationMap().remove(from));
//        }
//        return rtn;
//    }

    public void rebuildOrderedGlobalLocationMap() {
        orderedGlobalLocationMap = new TreeMap<>(new StringLengthComparator());
        if (nonNull(autoGlobalLocationMap))
            orderedGlobalLocationMap.putAll(autoGlobalLocationMap);
        // User list goes last to ensure precedence.
        if (nonNull(userGlobalLocationMap))
            orderedGlobalLocationMap.putAll(userGlobalLocationMap);
    }

    public void addTableSource(String database, String table, String tableType, String source, int consolidationLevelBase,
                               boolean partitionLevelMismatch) {
        if (isNull(warehouseMapBuilder))
            warehouseMapBuilder = new WarehouseMapBuilder();
        try {
            TableType type = TableType.valueOf(tableType);
            warehouseMapBuilder.addSourceLocation(database, table, type, null, source, null,
                    consolidationLevelBase, partitionLevelMismatch);
        } catch (IllegalArgumentException iae) {
            log.info("Not a supported table type: {}", tableType);
        }
    }

    public void addPartitionSource(String database, String table, String tableType, String partitionSpec,
                                   String tableSource, String partitionSource, int consolidationLevelBase,
                                   boolean partitionLevelMismatch) {
        if (isNull(warehouseMapBuilder))
            warehouseMapBuilder = new WarehouseMapBuilder();
        TableType type = TableType.valueOf(tableType);
        warehouseMapBuilder.addSourceLocation(database, table, type, partitionSpec, tableSource, partitionSource,
                consolidationLevelBase, partitionLevelMismatch);
    }

    public void removeDatabaseFromTranslationMap(String database) {
        translationMap.remove(database);
    }

    public synchronized void addTranslation(String database, Environment environment, String originalLocation, String newLocation, int level, boolean consolidationTablesForDistcp) {
        EnvironmentMap environmentMap = translationMap.computeIfAbsent(database, k -> new EnvironmentMap());
        environmentMap.addTranslationLocation(environment, originalLocation, newLocation, level, consolidationTablesForDistcp);
//        getDbLocationMap(database, environment).put(originalLocation, newLocation);
    }

    public synchronized Set<EnvironmentMap.TranslationLevel> getTranslationMap(String database, Environment environment) {
        EnvironmentMap envMap = translationMap.computeIfAbsent(database, k -> new EnvironmentMap());
        return envMap.getTranslationSet(environment);
    }

    // Needed to ensure the return is the ordered map.
//    public Map<String, String> getGlobalLocationMap() {
//        return getOrderedGlobalLocationMap();
//    }

    @JsonIgnore
    // This set is ordered by the length of the key in descending order
    // to ensure that the longest path is replaced first.
    public Map<String, Map<TableType, String>> getOrderedGlobalLocationMap() {
        if (isNull(orderedGlobalLocationMap) ||
                (orderedGlobalLocationMap.isEmpty()
                        && (nonNull(userGlobalLocationMap) || nonNull(autoGlobalLocationMap)))) {
            orderedGlobalLocationMap = new TreeMap<String, Map<TableType, String>>(new StringLengthComparator());
            // Add the global location map to the ordered map.
            if (nonNull(userGlobalLocationMap))
                orderedGlobalLocationMap.putAll(userGlobalLocationMap);
            if (nonNull(autoGlobalLocationMap))
                orderedGlobalLocationMap.putAll(autoGlobalLocationMap);

        }
        return orderedGlobalLocationMap;
    }

    public Boolean validate() {
        Boolean rtn = Boolean.TRUE;
        return rtn;
    }

}
