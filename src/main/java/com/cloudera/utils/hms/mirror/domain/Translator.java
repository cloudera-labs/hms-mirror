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

/**
 * Handles translation logic, location mapping, and tracks various mappings for table and partition sources
 * during warehouse planning and migration between environments.
 */
@Slf4j
@Getter
@Setter
@JsonIgnoreProperties(value = {"dbLocationMap"}, ignoreUnknown = true)
public class Translator implements Cloneable {

    /**
     * The main in-memory map for maintaining translation environments by database.
     * Key: database name.
     * Value: EnvironmentMap of translation locations and metadata.
     */
    @JsonIgnore
    private final Map<String, EnvironmentMap> translationMap = new TreeMap<>();

    /**
     * Force external location in table DDLs.
     * If true, table create statements will explicitly set location rather than rely on the database directory.
     */
    private boolean forceExternalLocation = Boolean.FALSE;

    /**
     * Auto-built global location map constructed by the system from warehouse plans.
     * Key: source path.
     * Value: Map from TableType to target location.
     */
    private Map<String, Map<TableType, String>> autoGlobalLocationMap = null;

    /**
     * User-provided global location map for custom translations.
     * Key: source path.
     * Value: Map from TableType to target location.
     */
    private Map<String, Map<TableType, String>> userGlobalLocationMap = null;

    /**
     * Ordered version of the global location map, sorted by key length for path replacement.
     * Combines both user and auto maps.
     */
    @JsonIgnore
    private Map<String, Map<TableType, String>> orderedGlobalLocationMap = null;

    /**
     * Builder and state tracker for warehouse mapping.
     */
    private WarehouseMapBuilder warehouseMapBuilder = new WarehouseMapBuilder();

    /**
     * Reset all session-specific state and clear relevant internal maps.
     */
    public void reset() {
        if (nonNull(translationMap)) translationMap.clear();
        if (nonNull(autoGlobalLocationMap)) autoGlobalLocationMap.clear();
        if (nonNull(orderedGlobalLocationMap)) orderedGlobalLocationMap.clear();
        if (nonNull(warehouseMapBuilder)) warehouseMapBuilder.reset();
    }

    /**
     * Add a new user global location mapping.
     * @param tableType The type of the table (e.g. MANAGED, EXTERNAL).
     * @param from      The source path.
     * @param to        The target translation path.
     */
    public void addUserGlobalLocationMap(TableType tableType, String from, String to) {
        if (isNull(userGlobalLocationMap)) userGlobalLocationMap = new HashMap<>();
        Map<TableType, String> target = userGlobalLocationMap.get(from);
        if (isNull(target)) target = new HashMap<>();
        target.put(tableType, to);
        userGlobalLocationMap.put(from, target);
        getOrderedGlobalLocationMap().put(from, target);
    }

    /**
     * Create a deep clone of Translator, duplicating user/auto maps and builder.
     * @return Cloned Translator object.
     */
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

    /**
     * Remove a user global location mapping or a specific TableType mapping.
     * @param from      The source path.
     * @param tableType Optional TableType; if null, removes all table types for this path.
     */
    public void removeUserGlobalLocationMap(String from, TableType tableType) {
        if (userGlobalLocationMap == null) return;
        if (isNull(tableType)) {
            userGlobalLocationMap.remove(from);
        } else {
            Map<TableType, String> target = userGlobalLocationMap.get(from);
            if (nonNull(target)) target.remove(tableType);
        }
    }

    /**
     * Rebuild the ordered global location map by merging user and auto maps, sorting keys by length.
     * User-provided entries take precedence.
     */
    public void rebuildOrderedGlobalLocationMap() {
        orderedGlobalLocationMap = new TreeMap<>(new StringLengthComparator());
        if (nonNull(autoGlobalLocationMap)) orderedGlobalLocationMap.putAll(autoGlobalLocationMap);
        if (nonNull(userGlobalLocationMap)) orderedGlobalLocationMap.putAll(userGlobalLocationMap);
    }

    /**
     * Add a table source and its properties to warehouse mapping.
     * @param database                 Database name.
     * @param table                    Table name.
     * @param tableType                Table type as string (converted to TableType enum).
     * @param source                   Source path.
     * @param consolidationLevelBase   Data consolidation/normalization level.
     * @param partitionLevelMismatch   If partitioning levels mismatch.
     */
    public void addTableSource(String database, String table, String tableType, String source,
                               int consolidationLevelBase, boolean partitionLevelMismatch) {
        if (isNull(warehouseMapBuilder)) warehouseMapBuilder = new WarehouseMapBuilder();
        try {
            TableType type = TableType.valueOf(tableType);
            warehouseMapBuilder.addSourceLocation(database, table, type, null, source, null,
                    consolidationLevelBase, partitionLevelMismatch);
        } catch (IllegalArgumentException iae) {
            log.info("Not a supported table type: {}", tableType);
        }
    }

    /**
     * Add a partition source and its properties to warehouse mapping.
     * @param database               Database name.
     * @param table                  Table name.
     * @param tableType              Table type as string (converted to TableType enum).
     * @param partitionSpec          Partition spec string.
     * @param tableSource            Table-level source.
     * @param partitionSource        Partition-level source.
     * @param consolidationLevelBase Data consolidation/normalization level.
     * @param partitionLevelMismatch If partitioning levels mismatch.
     */
    public void addPartitionSource(String database, String table, String tableType, String partitionSpec,
                                   String tableSource, String partitionSource, int consolidationLevelBase,
                                   boolean partitionLevelMismatch) {
        if (isNull(warehouseMapBuilder)) warehouseMapBuilder = new WarehouseMapBuilder();
        TableType type = TableType.valueOf(tableType);
        warehouseMapBuilder.addSourceLocation(database, table, type, partitionSpec, tableSource, partitionSource,
                consolidationLevelBase, partitionLevelMismatch);
    }

    /**
     * Remove a database and its mappings from the translation map.
     * @param database Database name.
     */
    public void removeDatabaseFromTranslationMap(String database) {
        translationMap.remove(database);
    }

    /**
     * Add a translation for a database/environment.
     * Synchronized to avoid concurrent modifications.
     * @param database                   Database name.
     * @param environment                Environment type.
     * @param originalLocation           Source path.
     * @param newLocation                Target path.
     * @param level                      Translation level indicator.
     * @param consolidationTablesForDistcp Flag for distcp consolidation.
     */
    public synchronized void addTranslation(String database, Environment environment,
                                            String originalLocation, String newLocation,
                                            int level, boolean consolidationTablesForDistcp) {
        EnvironmentMap environmentMap = translationMap.computeIfAbsent(database, k -> new EnvironmentMap());
        environmentMap.addTranslationLocation(environment, originalLocation, newLocation, level, consolidationTablesForDistcp);
    }

    /**
     * Retrieve the translation map for a given database and environment.
     * Synchronized to avoid concurrent modifications.
     *
     * @param database   Database name.
     * @param environment Environment type.
     * @return Set of translation levels for the given environment.
     */
    public synchronized Set<EnvironmentMap.TranslationLevel> getTranslationMap(String database, Environment environment) {
        EnvironmentMap envMap = translationMap.computeIfAbsent(database, k -> new EnvironmentMap());
        return envMap.getTranslationSet(environment);
    }

    /**
     * Get the ordered global location map, building as necessary.
     * Sorted by key length to ensure the longest path matches occur first.
     * Combines user and auto maps.
     *
     * @return Ordered global location map.
     */
    @JsonIgnore
    public Map<String, Map<TableType, String>> getOrderedGlobalLocationMap() {
        if (isNull(orderedGlobalLocationMap) ||
                (orderedGlobalLocationMap.isEmpty()
                        && (nonNull(userGlobalLocationMap) || nonNull(autoGlobalLocationMap)))) {
            orderedGlobalLocationMap = new TreeMap<>(new StringLengthComparator());
            if (nonNull(userGlobalLocationMap)) orderedGlobalLocationMap.putAll(userGlobalLocationMap);
            if (nonNull(autoGlobalLocationMap)) orderedGlobalLocationMap.putAll(autoGlobalLocationMap);
        }
        return orderedGlobalLocationMap;
    }

    /**
     * Validation hook. Currently always returns true.
     * @return true if the Translator is valid.
     */
    public Boolean validate() {
        return Boolean.TRUE;
    }
}