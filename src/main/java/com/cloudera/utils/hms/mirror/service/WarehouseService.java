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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.Warehouse;
import com.cloudera.utils.hms.mirror.domain.WarehouseMapBuilder;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

import static com.cloudera.utils.hms.mirror.MessageCode.WAREHOUSE_DIRECTORIES_NOT_DEFINED;
import static com.cloudera.utils.hms.mirror.MessageCode.WAREHOUSE_DIRECTORIES_RETRIEVED_FROM_HIVE_ENV;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Slf4j
@Getter
public class WarehouseService {
    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    public Warehouse addWarehousePlan(String database, String external, String managed) throws RequiredConfigurationException {
        if (isBlank(external) || isBlank(managed)) {
            throw new RequiredConfigurationException("External and Managed Warehouse Locations must be defined.");
        }
        if (external.equals(managed)) {
            throw new RequiredConfigurationException("External and Managed Warehouse Locations must be different.");
        }
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();
        hmsMirrorConfig.getDatabases().add(database);
        return warehouseMapBuilder.addWarehousePlan(database, external, managed);
    }

    public Warehouse removeWarehousePlan(String database) {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();
        hmsMirrorConfig.getDatabases().remove(database);
        return warehouseMapBuilder.removeWarehousePlan(database);
    }

    /*
    Look at the Warehouse Plans for a matching Database and pull that.  If that doesn't exist, then
    pull the general warehouse locations if they are defined.  If those aren't, try and pull the locations
    from the Hive Environment Variables

    A null returns means a warehouse couldn't be determined and the db location settings should be skipped.
     */
    public Warehouse getWarehousePlan(String database) throws MissingDataPointException {
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = config.getTranslator().getWarehouseMapBuilder();
        // Find it by a Warehouse Plan
        Warehouse warehouse = warehouseMapBuilder.getWarehousePlans().get(database);
        if (isNull(warehouse)) {
            ExecuteSession session = executeSessionService.getSession();
            // Get the default Warehouse defined for the config.
            if (nonNull(session.getConfig().getTransfer().getWarehouse())) {
                warehouse = session.getConfig().getTransfer().getWarehouse();
            }

            if (nonNull(warehouse) && (isBlank(warehouse.getExternalDirectory()) || isBlank(warehouse.getManagedDirectory()))) {
                warehouse = null;
            }

            if (isNull(warehouse)) {
                // Look for Location in the right DB Definition for Migration Strategies.
                switch (config.getDataStrategy()) {
                    case DUMP:
                        return null;
                    case SCHEMA_ONLY:
                    case EXPORT_IMPORT:
                    case HYBRID:
                    case SQL:
                    case COMMON:
                    case LINKED:
                        warehouse = config.getCluster(Environment.RIGHT).getEnvironmentWarehouse();
                        if (nonNull(warehouse)) {
                            session.addWarning(WAREHOUSE_DIRECTORIES_RETRIEVED_FROM_HIVE_ENV);
                        } else {
                            session.addWarning(WAREHOUSE_DIRECTORIES_NOT_DEFINED);
                        }
                        break;
                    default: // STORAGE_MIGRATION should set these manually.
                        session.addWarning(WAREHOUSE_DIRECTORIES_NOT_DEFINED);
                }
            }
        }

        if (isNull(warehouse)) {
            throw new MissingDataPointException("Warehouse Plan for Database: " + database + " not found and couldn't be built from (Warehouse Plans, General Warehouse Configs or Hive ENV.");
        }

        return warehouse;
    }

    public Map<String, Warehouse> getWarehousePlans() {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();
        return warehouseMapBuilder.getWarehousePlans();
    }

    public void clearWarehousePlan() {
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = config.getTranslator().getWarehouseMapBuilder();
        warehouseMapBuilder.clearWarehousePlan();
    }


}
