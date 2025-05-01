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

package com.cloudera.utils.hms.mirror.web.controller.api.v1.runtime;

import com.cloudera.utils.hms.mirror.domain.Warehouse;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.service.DatabaseService;
import com.cloudera.utils.hms.mirror.service.WarehouseService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

import static java.util.Objects.isNull;

@CrossOrigin
@RestController
@Slf4j
@RequestMapping(path = "/api/v1/database")
public class DatabaseController {

    private final DatabaseService databaseService;
    private final WarehouseService warehouseService;

    public DatabaseController(DatabaseService databaseService, WarehouseService warehouseService) {
        this.databaseService = databaseService;
        this.warehouseService = warehouseService;
    }

    @Operation(summary = "List available Databases")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Cluster Databases",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = List.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid environment supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/list")
    public List<String> listAvailableDatabases(@RequestParam(name = "environment", required = false) Environment environment) throws RuntimeException {
        Environment lclEnv = isNull(environment) ? Environment.LEFT : environment;
        return databaseService.listAvailableDatabases(lclEnv);
    }

    // Add a Warehouse Plan
    @Operation(summary = "Add a Warehouse Plan for a specific database")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Warehouse Plan added successfully",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Warehouse.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/{database}/warehousePlan")
    public Warehouse addWarehousePlan(@PathVariable @NotNull String database,
                                      @RequestParam(name = "externalLocation", required = true) String externalLocation,
                                      @RequestParam(name = "managedLocation", required = true) String managedLocation)
            throws RequiredConfigurationException {
        return warehouseService.addWarehousePlan(database, externalLocation, managedLocation);
    }

    // Remove a Warehouse Plan
    @Operation(summary = "Remove a Warehouse Plan for a specific database")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Warehouse Plan removed successfully",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Warehouse.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.DELETE, value = "/{database}/warehousePlan")
    public Warehouse removeWarehousePlan(@PathVariable @NotNull String database) {
        return warehouseService.removeWarehousePlan(database);
    }

    // Get a Warehouse Plan
    @Operation(summary = "Get a Warehouse Plan for a specific database")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Warehouse Plan removed successfully",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Warehouse.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/{database}/warehousePlan")
    public Warehouse getWarehousePlan(@PathVariable @NotNull String database) {
        try {
            return warehouseService.getWarehousePlan(database);
        } catch (MissingDataPointException e) {
            log.error("Error getting Warehouse Plan for database: " + database, e);
            return null;
        }
    }

    // Get all Warehouse Plans
    @Operation(summary = "Get Warehouse Plans")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Warehouse Plans retrieved successfully",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Map.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/warehousePlan/list")
    public Map<String, Warehouse> getWarehousePlans() {
        return warehouseService.getWarehousePlans();
    }

    // The commented-out methods can remain unchanged if/when you uncomment them,
    // as their service field usage is already compatible with constructor injection.

}