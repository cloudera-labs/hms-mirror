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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties({"downgradeInPlace"})
@Schema(description = "Migrate ACID table configurations")
public class MigrateACID implements Cloneable {

    /*
    Whether or not we'll be migrating ACID tables.
     */
    @Schema(description = "Whether or not we'll be migrating ACID tables.")
    private boolean on = Boolean.FALSE;

    @Schema(description = "Whether or not we'll be migrating ACID tables ONLY. This option will exclude non-ACID tables from migration.")
    private boolean only = Boolean.FALSE;

    /*
    When migrating ACID tables, older ACID tables (Hive 1/2) required buckets in the definition.  In Hive 3, this is no longer
    required.  The option provides a means to rewrite the definition so that buckets don't need to be a part of the definition.
    This is helpful when the bucket definition was required but not a part of design and lead to an artificial design element.

    The default of 2 means that any definition we find which 2 buckets or less will be removed to NO bucket strategy in the new
    definition.


    Set this value to -1 to eliminate this check and translate the bucket definitions as they are.
     */
    @Schema(description = "Threshold for the number of buckets in the table definition to consider removing the bucket definition.")
    private Integer artificialBucketThreshold = 2;

    /*
    Migrating ACID tables through a transfer table is the only way to convert ACIDv1 tables to ACIDv2.  The EXPORT_IMPORT
    process between Hive 1/2 isn't compatible with Hive 3.  So this process will create a staging table for the transfer.
    If there are too many partitions the process of transferring data from the source to the transfer table AND later, the
    final ACID table, can be problematic.  Mostly because of resources or limits on containers and AM and Task memory sizes.

    This value allow us to set a value that limits the size of the partitioned dataset we'll attempt to address.  When the
    number of partitions for the source table exceeds this value, we won't even try to attempt this.
     */
    @Schema(description = "Limit the number of partitions in a table to migrate.")
    private Integer partitionLimit = 500;

    /*
    Downgrade ACID tables to EXTERNAL w/ purge tables.
     */
    @Schema(description = "Downgrade ACID tables to EXTERNAL w/ purge tables.")
    private boolean downgrade = Boolean.FALSE;
    /*
    When 'in-place', only the LEFT cluster is used and the ACID tables are downgraded in-place.  In-place means within the
    same cluster.  SQL/EXPORT_IMPORT will be used to migrate the data out of the ACID table, into an EXTERNAL/PURGE table.  That table
    will be renamed to the original ACID table once this is completed.
     */
    @Schema(description = "ACID tables in-place.")
    private boolean inplace = Boolean.FALSE;

    @Override
    public MigrateACID clone() {
        try {
            MigrateACID clone = (MigrateACID) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    public void setInplace(boolean inplace) {
        this.inplace = inplace;
        if (this.inplace) {
            if (!on) {
                this.on = Boolean.TRUE;
            }
            if (!only) {
                this.only = Boolean.TRUE;
            }
        }
    }

    public void setDowngrade(boolean downgrade) {
        this.downgrade = downgrade;
        if (this.downgrade) {
            if (!on) {
                this.on = Boolean.TRUE;
            }
            if (!only) {
                this.only = Boolean.TRUE;
            }
        }
    }

}
