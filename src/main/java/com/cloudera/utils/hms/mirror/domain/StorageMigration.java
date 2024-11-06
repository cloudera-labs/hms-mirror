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

import com.cloudera.utils.hms.mirror.domain.support.DataMovementStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.DistcpFlowEnum;
import com.cloudera.utils.hms.mirror.domain.support.TranslationTypeEnum;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Schema(description = "Storage migration configuration that controls the data movement strategy")
public class StorageMigration implements Cloneable {

    @Schema(description = "The type of translation to use to migrate locations")
    private TranslationTypeEnum translationType = TranslationTypeEnum.RELATIVE;
    @Schema(description = "Data movement strategy")
    private DataMovementStrategyEnum dataMovementStrategy = DataMovementStrategyEnum.SQL;
    @Schema(description = "Data flow direction for distcp. This control from where the 'distcp' jobs should be run.")
    private DistcpFlowEnum dataFlow = DistcpFlowEnum.PULL;

    @Schema(description = "When true, the database location will NOT be adjusted to match the table locations that are migrated. " +
            "This is useful in the case of an 'archive' strategy where you only want to migrate the table(s) but are not yet " +
            "ready to migration or stage future tables at the new location.")
    private boolean skipDatabaseLocationAdjustments = Boolean.FALSE;

    @Schema(description = "When true (default is false), the tables being migrated will be archived vs. simply changing the location details " +
            "of the tables metadata.  This is relevant only for STORAGE_MIGRATION when using the 'DISTCP' data movement " +
            "strategy. When the data movement strategy is 'SQL' for STORAGE_MIGRATION, this flag is ignored because the default " +
            "behavior is to create an archive table anyhow.")
    private boolean createArchive = Boolean.FALSE;

    @Schema(description = "When true, the tables will be consolidated into a single directory for distcp. " +
            "This is useful when the source tables are spread across multiple directories.")
    private boolean consolidateTablesForDistcp = Boolean.FALSE;
    @Schema(description = "When strict is true, any issues during evaluation will cause the migration to fail. When false, " +
            "the migration will continue but the issues will be reported. This can lead to data movement issues.")
    private boolean strict = Boolean.TRUE;

    @Override
    public StorageMigration clone() {
        try {
            StorageMigration clone = (StorageMigration) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    @JsonIgnore
    public boolean isDistcp() {
        return dataMovementStrategy == DataMovementStrategyEnum.DISTCP;
    }
}
