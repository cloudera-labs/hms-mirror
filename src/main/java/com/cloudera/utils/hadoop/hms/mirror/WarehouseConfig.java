/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms.mirror;

public class WarehouseConfig {
    private String managedDirectory = null;
    private String externalDirectory = null;

    public String getManagedDirectory() {
        return managedDirectory;
    }

    public void setManagedDirectory(String managedDirectory) {
        if (managedDirectory != null) {
            this.managedDirectory = managedDirectory.trim();
            if (!this.managedDirectory.startsWith("/")) {
                this.managedDirectory = "/" + this.managedDirectory;
            }
            if (this.managedDirectory.endsWith("/")) {
                this.managedDirectory = this.managedDirectory.substring(0, this.managedDirectory.length() - 1);
            }
        } else {
            this.managedDirectory = managedDirectory;
        }
    }

    public String getExternalDirectory() {
        return externalDirectory;
    }

    public void setExternalDirectory(String externalDirectory) {
        if (externalDirectory != null) {
            this.externalDirectory = externalDirectory.trim();
            if (!this.externalDirectory.startsWith("/")) {
                this.externalDirectory = "/" + this.externalDirectory;
            }
            if (this.externalDirectory.endsWith("/")) {
                this.externalDirectory = this.externalDirectory.substring(0, this.externalDirectory.length() - 1);
            }
        } else {
            this.externalDirectory = externalDirectory;
        }
    }
}
