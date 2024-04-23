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

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/*
Used to define the table type and location you want to create.
 */
@Slf4j
@Getter
@Setter
public class CopySpec {
    private TableMirror tableMirror = null;
    private Environment target = null;
    private Environment source = null;
    /*
    When specified, the table should be upgraded if it is a legacy managed table.
     */
    private boolean upgrade = Boolean.FALSE;
    private boolean makeExternal = Boolean.FALSE;
    private boolean makeNonTransactional = Boolean.FALSE;
    private boolean stripLocation = Boolean.FALSE;
    private boolean replaceLocation = Boolean.FALSE;
    private boolean takeOwnership = Boolean.FALSE;
    private String tableNamePrefix = null;
    private String location = null;

    public CopySpec(TableMirror tableMirror, Environment source, Environment target) {
        this.tableMirror = tableMirror;
        this.target = target;
        this.source = source;
    }

    public boolean renameTable() {
        if (tableNamePrefix != null)
            return Boolean.TRUE;
        else
            return Boolean.FALSE;
    }

    public void setUpgrade(Boolean upgrade) {
        this.upgrade = upgrade;
        if (this.upgrade) {
            takeOwnership = Boolean.TRUE;
        }
    }

}
