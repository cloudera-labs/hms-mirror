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
import org.springframework.stereotype.Service;

import java.util.Map;

import static com.cloudera.utils.hms.mirror.MessageCode.WAREHOUSE_DIRECTORIES_NOT_DEFINED;
import static com.cloudera.utils.hms.mirror.MessageCode.WAREHOUSE_DIRECTORIES_RETRIEVED_FROM_HIVE_ENV;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Service for managing warehouse plans associated with Hive databases.
 * Provides methods for adding, removing, retrieving, and clearing warehouse plans.
 */
@Service
@Slf4j
@Getter
public class WarehouseService {

    private static final String EXTERNAL_AND_MANAGED_LOCATIONS_REQUIRED =
            "External and Managed Warehouse Locations must be defined.";
    private static final String EXTERNAL_AND_MANAGED_LOCATIONS_DIFFERENT =
            "External and Managed Warehouse Locations must be different.";
    private static final String WAREHOUSE_PLAN_NOT_FOUND_MSG =
            "Warehouse Plan for Database: %s not found and couldn't be built from (Warehouse Plans, General Warehouse Configs or Hive ENV.";

    private final ExecuteSessionService executeSessionService;

    /**
     * Constructs a WarehouseService with the given ExecuteSessionService.
     *
     * @param executeSessionService the service for managing execution sessions.
     */
    public WarehouseService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
        log.debug("WarehouseService initialized.");
    }

    /**
     * Adds a warehouse plan for a given database.
     *
     * @param database the name of the database.
     * @param external the external warehouse location.
     * @param managed  the managed warehouse location.
     * @return the created {@link Warehouse} plan.
     * @throws RequiredConfigurationException if external or managed locations are blank or identical.
     */
    public Warehouse addWarehousePlan(String database, String external, String managed)
            throws RequiredConfigurationException {
        if (isBlank(external) || isBlank(managed)) {
            throw new RequiredConfigurationException(EXTERNAL_AND_MANAGED_LOCATIONS_REQUIRED);
        }
        if (external.equals(managed)) {
            throw new RequiredConfigurationException(EXTERNAL_AND_MANAGED_LOCATIONS_DIFFERENT);
        }
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder mapBuilder = getWarehouseMapBuilder(config);
        mapBuilder.addWarehousePlan(database, external, managed);
        config.getDatabases().add(database);
        return mapBuilder.addWarehousePlan(database, external, managed);
    }

    /**
     * Removes a warehouse plan for a given database.
     *
     * @param database the name of the database whose warehouse plan will be removed.
     * @return the removed {@link Warehouse} plan, or null if not found.
     */
    public Warehouse removeWarehousePlan(String database) {
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        getWarehouseMapBuilder(config).removeWarehousePlan(database);
        config.getDatabases().remove(database);
        return getWarehouseMapBuilder(config).removeWarehousePlan(database);
    }

    /**
     * Retrieves a warehouse plan for the given database, with fallbacks to configuration or hive environment.
     *
     * @param database the name of the database.
     * @return the {@link Warehouse} plan for this database.
     * @throws MissingDataPointException if a plan cannot be found or built using available configuration.
     */
    public Warehouse getWarehousePlan(String database) throws MissingDataPointException {
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = getWarehouseMapBuilder(config);
        Warehouse warehouse = warehouseMapBuilder.getWarehousePlans().get(database);

        if (isNull(warehouse)) {
            ExecuteSession session = executeSessionService.getSession();
            if (nonNull(session.getConfig().getTransfer().getWarehouse())) {
                warehouse = session.getConfig().getTransfer().getWarehouse();
            }
            if (nonNull(warehouse) &&
                    (isBlank(warehouse.getExternalDirectory()) || isBlank(warehouse.getManagedDirectory()))) {
                warehouse = null;
            }
            if (isNull(warehouse)) {
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
                    default:
                        session.addWarning(WAREHOUSE_DIRECTORIES_NOT_DEFINED);
                }
            }
        }

        if (isNull(warehouse)) {
            throw new MissingDataPointException(String.format(WAREHOUSE_PLAN_NOT_FOUND_MSG, database));
        }
        return warehouse;
    }

    /**
     * Gets all defined warehouse plans.
     *
     * @return a map of database names to {@link Warehouse} plans.
     */
    public Map<String, Warehouse> getWarehousePlans() {
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        return getWarehouseMapBuilder(config).getWarehousePlans();
    }

    /**
     * Removes all defined warehouse plans.
     */
    public void clearWarehousePlans() {
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        getWarehouseMapBuilder(config).clearWarehousePlan();
    }

    /**
     * Retrieves the {@link WarehouseMapBuilder} instance from the configuration.
     *
     * @param config the HMS mirror configuration.
     * @return the {@link WarehouseMapBuilder} for the current configuration.
     */
    private WarehouseMapBuilder getWarehouseMapBuilder(HmsMirrorConfig config) {
        return config.getTranslator().getWarehouseMapBuilder();
    }
}