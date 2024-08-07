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
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Getter
@Setter
public class SourceLocationMap implements Cloneable {
    /*
    A map of locations for a given table type.

    The key is the table type and the Map is a map of locations(key) and tables(value.set).

     */
    private Map<TableType, Map<String, Set<String>>> locations = new HashMap<>();

    public Map<String, Set<String>> get(TableType type) {
        if (locations.containsKey(type)) {
            return locations.get(type);
        } else {
            Map<String, Set<String>> locationMap = new HashMap<>();
            locations.put(type, locationMap);
            return locationMap;
        }
    }

    public void addTableLocation(String table, TableType type, String location) {
        Map<String, Set<String>> locationMap = get(type);

        if (locationMap.containsKey(location)) {
            locationMap.get(location).add(table);
        } else {
            Set<String> tables = new HashSet<>();
            tables.add(table);
            locationMap.put(location, tables);
        }
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        SourceLocationMap clone = (SourceLocationMap)super.clone();
        // deep copy
        Map<TableType, Map<String, Set<String>>> newLocations = new HashMap<>();

        for (Map.Entry<TableType, Map<String, Set<String>>> entry : locations.entrySet()) {
            Map<String, Set<String>> newEntry = new HashMap<>();
            for (Map.Entry<String, Set<String>> entry2 : entry.getValue().entrySet()) {
                newEntry.put(entry2.getKey(), new HashSet<>(entry2.getValue()));
            }
            newLocations.put(entry.getKey(), newEntry);
        }
        clone.setLocations(newLocations);
        return clone;
    }
}
