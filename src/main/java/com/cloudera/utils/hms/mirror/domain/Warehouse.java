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

import com.cloudera.utils.hms.mirror.domain.support.WarehouseSource;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import static org.apache.commons.lang3.StringUtils.isBlank;

/*
The Warehouse Base is the location base for a databases external and managedDirectory locations. The locations
should NOT include the database name.  The database name will be appended to the location when the process
is run.
 */
@Getter
public class Warehouse implements Cloneable {


    @Setter
    @Schema(description = "The source of the warehouse location.")
    private WarehouseSource source = WarehouseSource.GLOBAL;
    @Schema(description = "The external directory location for the database.  This directory should NOT contain the database name, " +
            "the database name will be appended to the location when the process is run.")
    private String externalDirectory;
    @Schema(description = "The managed directory location for the database.  This directory should NOT contain the database name, " +
            "the database name will be appended to the location when the process is run.")
    private String managedDirectory;

    public Warehouse() {
    }

    public Warehouse(WarehouseSource source, String externalDirectory, String managedDirectory) {
        this.source = source;
        this.externalDirectory = externalDirectory;
        this.managedDirectory = managedDirectory;
    }

    public void setExternalDirectory(String externalDirectory) {
        if (!isBlank(externalDirectory)) {
            this.externalDirectory = externalDirectory.trim();
            if (!this.externalDirectory.startsWith("/")) {
                this.externalDirectory = "/" + this.externalDirectory;
            }
            if (this.externalDirectory.endsWith("/")) {
                this.externalDirectory = this.externalDirectory.substring(0, this.externalDirectory.length() - 1);
            }
        } else {
            this.externalDirectory = null;
        }
    }

    public void setManagedDirectory(String managedDirectory) {
        if (!isBlank(managedDirectory)) {
            this.managedDirectory = managedDirectory.trim();
            if (!this.managedDirectory.startsWith("/")) {
                this.managedDirectory = "/" + this.managedDirectory;
            }
            if (this.managedDirectory.endsWith("/")) {
                this.managedDirectory = this.managedDirectory.substring(0, this.managedDirectory.length() - 1);
            }
        } else {
            this.managedDirectory = null;
        }
    }

    @Override
    protected Warehouse clone() throws CloneNotSupportedException {
        return (Warehouse)super.clone();
    }
}
