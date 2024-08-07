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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.util.regex.Pattern;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Getter
@Setter
public class Filter implements Cloneable {
    @JsonIgnore
    private Pattern dbFilterPattern = null;
    @JsonIgnore // wip
    private String dbRegEx = null;
    @JsonIgnore
    private Pattern tblExcludeFilterPattern = null;
    @JsonIgnore
    private Pattern tblFilterPattern = null;
    @Schema(description = "Regular expression of tables to 'exclude' from the list")
    private String tblExcludeRegEx = null;
    @Schema(description = "Regular expression of tables to include in the list")
    private String tblRegEx = null;
    @Schema(description = "Maximum size of a table in bytes (-1 for no limit)")
    private Long tblSizeLimit = -1L;
    @Schema(description = "Maximum number of partitions in a table (-1 for no limit)")
    private Integer tblPartitionLimit = -1;

    @Override
    public Filter clone() {
        try {
            Filter clone = (Filter) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

//    public void setDbRegEx(String dbRegEx) {
//        this.dbRegEx = dbRegEx;
//    }

    public Pattern getDbFilterPattern() {
        if (!isBlank(dbRegEx) && isNull(dbFilterPattern)) {
            dbFilterPattern = Pattern.compile(dbRegEx);
        }
        return dbFilterPattern;
    }

    public Pattern getTblFilterPattern() {
        if (!isBlank(tblRegEx) && isNull(tblFilterPattern)) {
            tblFilterPattern = Pattern.compile(tblRegEx);
        }
        return tblFilterPattern;
    }

    public Pattern getTblExcludeFilterPattern() {
        if (!isBlank(tblExcludeRegEx) && isNull(tblExcludeFilterPattern)) {
            tblExcludeFilterPattern = Pattern.compile(tblExcludeRegEx);
        }
        return tblExcludeFilterPattern;
    }

    @JsonIgnore
    public boolean isTableFiltering() {
        if (!isBlank(tblRegEx) || !isBlank(tblExcludeRegEx)) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }
}
