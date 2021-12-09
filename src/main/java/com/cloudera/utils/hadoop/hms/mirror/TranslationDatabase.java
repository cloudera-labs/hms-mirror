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

import java.util.Map;

public class TranslationDatabase {
    /*
    Optional, when defined, rename the db to this new name.
     */
    private String rename;
    /*
    Optional, when defined, set the db location.
     */
    private String location;
    /*
    Optional, when defined, set the db managed location.
    only relevant for non-legacy environments.
     */
    private String managedLocation;
    /*
    For all external (hive3) or legacy managed we need to pull
    all the data under the defined db location.
     */
    private Boolean consolidateExternal = Boolean.FALSE;
    /*
    Optional, map of tables and directives to apply.  When a
    table isn't defined here, nothing will be done from a
    translation perspective.
     */
    private Map<String, TranslationTable> tables;

    public String getRename() {
        return rename;
    }

    public void setRename(String rename) {
        this.rename = rename;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getManagedLocation() {
        return managedLocation;
    }

    public void setManagedLocation(String managedLocation) {
        this.managedLocation = managedLocation;
    }

    public Boolean getConsolidateExternal() {
        return consolidateExternal;
    }

    public void setConsolidateExternal(Boolean consolidateExternal) {
        this.consolidateExternal = consolidateExternal;
    }

    public Map<String, TranslationTable> getTables() {
        return tables;
    }

    public void setTables(Map<String, TranslationTable> tables) {
        this.tables = tables;
    }
}
