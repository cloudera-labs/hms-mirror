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
 */

package com.cloudera.utils.hms.mirror.datastrategy;

import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.StatsCalculatorService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Data strategy for handling ACID (Atomicity, Consistency, Isolation, Durability) table operations.
 * This strategy outlines methods required for building and executing migration tasks related to ACID tables.
 * All methods currently return {@code null} as placeholders for actual implementation.
 *
 * @author Cloudera
 * @since 2023-2024
 */
@Component
@Slf4j
public class AcidDataStrategy extends DataStrategyBase {

    /**
     * Constructs a new instance of {@code AcidDataStrategy}.
     *
     * @param statsCalculatorService the service responsible for statistics calculation
     * @param executeSessionService the service for executing sessions/queries
     * @param translatorService the service handling translation/mapping logic
     */
    public AcidDataStrategy(StatsCalculatorService statsCalculatorService,
                            ExecuteSessionService executeSessionService,
                            TranslatorService translatorService) {
        super(statsCalculatorService, executeSessionService, translatorService);
    }

    /**
     * Builds the target (destination) table or schema definition for an ACID table.
     *
     * @param tableMirror the table to build the definition for
     * @return {@code null} (method not implemented)
     */
    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        return null;
    }

    /**
     * Generates the SQL statements required to create or migrate an ACID table.
     *
     * @param tableMirror the table to build SQL for
     * @return {@code null} (method not implemented)
     * @throws MissingDataPointException if required data points are missing
     */
    @Override
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        return null;
    }

    /**
     * Executes the build process for an ACID table. This can include copying or transforming data structures.
     *
     * @param tableMirror the table to build
     * @return {@code null} (method not implemented)
     */
    @Override
    public Boolean build(TableMirror tableMirror) {
        return null;
    }

    /**
     * Executes the migration or transformation for an ACID table. This can include data transfer or applying changes.
     *
     * @param tableMirror the table to execute migration for
     * @return {@code null} (method not implemented)
     */
    @Override
    public Boolean execute(TableMirror tableMirror) {
        return null;
    }
}