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

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;

import java.util.regex.Pattern;

@Getter
@Setter
public class Filter {
    @JsonIgnore
    private Pattern dbFilterPattern = null;
    @JsonIgnore // wip
    private String dbRegEx = null;
    @JsonIgnore
    private Pattern tblExcludeFilterPattern = null;
    @JsonIgnore
    private Pattern tblFilterPattern = null;
    private String tblExcludeRegEx = null;
    private String tblRegEx = null;
    private Long tblSizeLimit = -1L;
    private Integer tblPartitionLimit = -1;

    public void setDbRegEx(String dbRegEx) {
        this.dbRegEx = dbRegEx;
        if (this.dbRegEx != null)
            dbFilterPattern = Pattern.compile(dbRegEx);
        else
            dbFilterPattern = null;

    }

    public void setTblExcludeRegEx(String tblExcludeRegEx) {
        this.tblExcludeRegEx = tblExcludeRegEx;
        if (this.tblExcludeRegEx != null)
            tblExcludeFilterPattern = Pattern.compile(tblExcludeRegEx);
        else
            tblExcludeFilterPattern = null;

    }

    public void setTblRegEx(String tblRegEx) {
        this.tblRegEx = tblRegEx;
        if (this.tblRegEx != null)
            tblFilterPattern = Pattern.compile(tblRegEx);
        else
            tblFilterPattern = null;
    }

    @JsonIgnore
    public boolean isTableFiltering() {
        if (tblRegEx != null || tblExcludeRegEx != null) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }
}
