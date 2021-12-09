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

/*
Used to define the table type and location you want to create.
 */
public class CopySpec {
    private Config config = null;
    private Environment target = null;
    private Environment source = null;
    /*
    When specified, the table should be upgraded if it is a legacy managed table.
     */
    private Boolean upgrade = Boolean.FALSE;
    private Boolean makeExternal = Boolean.FALSE;
    private Boolean makeNonTransactional = Boolean.FALSE;
    private Boolean stripLocation = Boolean.FALSE;
    private Boolean replaceLocation = Boolean.FALSE;
    private Boolean takeOwnership = Boolean.FALSE;
    private String tableNamePrefix = null;
    private String location = null;

    public CopySpec(Config config, Environment source, Environment target) {
        this.config = config;
        this.target = target;
        this.source = source;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public Environment getTarget() {
        return target;
    }

    public void setTarget(Environment target) {
        this.target = target;
    }

    public Environment getSource() {
        return source;
    }

    public void setSource(Environment source) {
        this.source = source;
    }

    public Boolean isMakeExternal() {
        return makeExternal;
    }

    public void setMakeExternal(Boolean makeExternal) {
        this.makeExternal = makeExternal;
    }

    public Boolean isMakeNonTransactional() {
        return makeNonTransactional;
    }

    public void setMakeNonTransactional(Boolean makeNonTransactional) {
        this.makeNonTransactional = makeNonTransactional;
    }

    public Boolean getStripLocation() {
        return stripLocation;
    }

    public void setStripLocation(Boolean stripLocation) {
        this.stripLocation = stripLocation;
    }

    public Boolean getReplaceLocation() {
        return replaceLocation;
    }

    public void setReplaceLocation(Boolean replaceLocation) {
        this.replaceLocation = replaceLocation;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public boolean renameTable() {
        if (tableNamePrefix != null)
            return Boolean.TRUE;
        else
            return Boolean.FALSE;
    }

    public Boolean getUpgrade() {
        return upgrade;
    }

    public void setUpgrade(Boolean upgrade) {
        this.upgrade = upgrade;
        if (this.upgrade) {
            takeOwnership = Boolean.TRUE;
        }
    }

    public Boolean getTakeOwnership() {
        return takeOwnership;
    }

    public void setTakeOwnership(Boolean takeOwnership) {
        this.takeOwnership = takeOwnership;
    }

    public String getTableNamePrefix() {
        return tableNamePrefix;
    }

    public void setTableNamePrefix(String tableNamePrefix) {
        this.tableNamePrefix = tableNamePrefix;
    }

}
