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

//@Component("transfer")
@Slf4j
@Getter
@Setter
public class TransferConfig {
    private int concurrency = 4;
    private String transferPrefix = "hms_mirror_transfer_";
    private String shadowPrefix = "hms_mirror_shadow_";
    private String exportBaseDirPrefix = "/apps/hive/warehouse/export_";
    private String remoteWorkingDirectory = "hms_mirror_working";
    private String intermediateStorage = null;
    private String commonStorage = null;
    private StorageMigration storageMigration = null;
    private WarehouseConfig warehouse = null;

    public StorageMigration getStorageMigration() {
        if (storageMigration == null)
            storageMigration = new StorageMigration();
        return storageMigration;
    }

    public WarehouseConfig getWarehouse() {
        if (warehouse == null)
            warehouse = new WarehouseConfig();
        return warehouse;
    }

    public void setCommonStorage(String commonStorage) {
        if (commonStorage != null) {
            this.commonStorage = commonStorage.trim();
            if (this.commonStorage.endsWith("/")) {
                // Remove trailing slash.
                this.commonStorage = this.commonStorage.substring(0, this.commonStorage.length() - 1);
            }
        } else {
            this.commonStorage = null;
        }
    }

}
