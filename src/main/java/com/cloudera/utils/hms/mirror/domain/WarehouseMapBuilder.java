/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

import com.cloudera.utils.hms.mirror.domain.support.TableType;
import com.cloudera.utils.hms.mirror.domain.support.WarehouseSource;
import com.cloudera.utils.hms.util.UrlUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Getter
@Setter
@Slf4j
public class WarehouseMapBuilder implements Cloneable {

    /*
    A map of Databases with a map of location translations.

    The key is the database name. The value is a map of 'location' and 'sources (list)'.

     */
    private Map<String, SourceLocationMap> sources = new HashMap<>();

    // Flag to track whether the sources have been reconciled and match the warehousePlans.
    private boolean inSync = Boolean.FALSE;

//    private Map<String, Map<String, String>> reconciledLocations = new HashMap<>();

    /*
    Use this to gather what the user desires for the final database location. The map key is the
    database and the value is the desired location.

    TODO: Does the location need to be include the database name or should it just be the 'base' location?
        - I think it should be the base location.

     */
    private Map<String, Warehouse> warehousePlans = new HashMap<>();

    public void reset() {
        if (nonNull(sources))
            sources.clear();
        inSync = Boolean.FALSE;
    }

    public Map<String, Warehouse> getWarehousePlans() {
        if (isNull(warehousePlans))
            warehousePlans = new HashMap<>();
        return warehousePlans;
    }

    public void clearWarehousePlan() {
        warehousePlans.clear();
    }

    public void clearSources() {
        sources.clear();
    }

    /*
    This is the STARTING point for the user to add databases and there desired locations to the process.

    The locations are the 'base' locations.  This means that the location does NOT include the database name.
     */
    public Warehouse addWarehousePlan(String database, String externalBaseLocation, String managedBaseLocation) {
        // Check the incoming directories and if the database name is included, remove it.
        if (externalBaseLocation.endsWith("/" + database + ".db")) {
            log.warn("The externalBaseLocation includes the database name.  Removing the database name from the location. " +
                    "It will be added back when the process is run.");
            externalBaseLocation = externalBaseLocation.substring(0, externalBaseLocation.length() - database.length() - 1);
        }
        if (managedBaseLocation.endsWith("/" + database + ".db")) {
            log.warn("The managedBaseLocation includes the database name.  Removing the database name from the location. " +
                    "It will be added back when the process is run.");
            managedBaseLocation = managedBaseLocation.substring(0, managedBaseLocation.length() - database.length() - 1);
        }
        Warehouse warehouseBase = new Warehouse(WarehouseSource.PLAN, externalBaseLocation, managedBaseLocation);
        /*
        Add the database and the desired warehousebase to the map.
         */
        inSync = Boolean.FALSE;
        return warehousePlans.put(database, warehouseBase);
    }

    public Warehouse removeWarehousePlan(String database) {
        inSync = Boolean.FALSE;
        return warehousePlans.remove(database);
    }

    /*
    From the gathered sources, build the initial reconciled locations map.  The 'values' aren't populated, as those
    are meant to be populated by the 'user'.
     */
//    public Map<String, Map<String, String>> buildReconciledLocationsFromSources() {
//        // Clear the reconciled locations.
//        reconciledLocations.clear();
//        // Iterate over the sources and build the reconciled locations.
//        for (Map.Entry<String, Map<String, Set<String>>> databaseEntry : sources.entrySet()) {
//            String database = databaseEntry.getKey();
//            Map<String, Set<String>> locationMap = databaseEntry.getValue();
//            Map<String, String> reconciledLocationMap = new TreeMap<>();
//            for (Map.Entry<String, Set<String>> locationEntry : locationMap.entrySet()) {
//                String location = locationEntry.getKey();
////                Set<String> sources = locationEntry.getValue();
////                String reconciledLocation = UrlUtils.reduceUrlBy(location, 0);
//                reconciledLocationMap.put(location, null); //String.join(",", sources));
//            }
//            reconciledLocations.put(database, reconciledLocationMap);
//        }
//        return reconciledLocations;
//    }

    public void addSourceLocation(String database, String table, TableType tableType, String partitionSpec, String tableLocation,
                                  String partLocation, int consolidationLevelBase, boolean partitionLevelMismatch) {
        SourceLocationMap locationMap = sources.computeIfAbsent(database, k -> new SourceLocationMap());

        String reducedLocation = null;

        if (isBlank(partitionSpec)) {
            // Process Table Source
            reducedLocation = UrlUtils.reduceUrlBy(tableLocation, consolidationLevelBase);
        } else {
            String[] partsSpec = partitionSpec.split("\\/");
            // Need to see if the partition location is base on the table location.
            if (partLocation.startsWith(tableLocation)) {
                // The partition location is based on the table location.
                // We shouldn't need to do anything here.  The table location will be used
                //    and that will capture the partition location in the GLM.
                return;
            } else {
                // The partition location is different than the table location.
                // We need to add the partition location to the GLM.
                if (partitionLevelMismatch) {
                    // The partition level mismatch is set. We need to reduce the partition location
                    //    to the base level.
                    reducedLocation = UrlUtils.reduceUrlBy(partLocation, consolidationLevelBase);
                } else {
                    // The partition level mismatch is not set. We need to reduce the partition location
                    //    to the partition level.
                    reducedLocation = UrlUtils.reduceUrlBy(partLocation, partsSpec.length + consolidationLevelBase);
                }
            }
        }

        locationMap.addTableLocation(table, tableType, reducedLocation);

    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        WarehouseMapBuilder clone = new WarehouseMapBuilder();
        // deep copy
        Map<String, SourceLocationMap> newSources = new HashMap<>();
        for (Map.Entry<String, SourceLocationMap> entry : sources.entrySet()) {
            newSources.put(entry.getKey(), (SourceLocationMap) entry.getValue().clone());
        }
        clone.setSources(newSources);

        Map<String, Warehouse> newWarehousePlan = new HashMap<>();
        for (Map.Entry<String, Warehouse> entry : warehousePlans.entrySet()) {
            newWarehousePlan.put(entry.getKey(), entry.getValue().clone());
        }
        clone.setWarehousePlans(newWarehousePlan);

        return super.clone();
    }
}
