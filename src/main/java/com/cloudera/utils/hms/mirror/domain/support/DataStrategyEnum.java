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

package com.cloudera.utils.hms.mirror.domain.support;

import com.cloudera.utils.hms.mirror.datastrategy.*;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public enum DataStrategyEnum {
    /*
    ACID Transfer is a hidden strategy applied to the table.
     */
    ACID(Boolean.TRUE, AcidDataStrategy.class, "acid"),
    /*
    The DUMP strategy will run against the LEFT cluster (attaching to it) and build scripts
     based on the configuration in that cluster.  You can optionally use the -f|--flip
     option to use the RIGHT cluster as the target for the dump.
     */
    DUMP(Boolean.FALSE, DumpDataStrategy.class, "dump"),
    /*
    The DOWNGRADE strategy is intended to downgrade ACID tables to EXTERNAL/PURGE tables on the
    LEFT cluster.  This process will only run against ACID tables (implies -mao|--migrate-acid-only).

    The process will create an alternate table in the same database with the same structure, but it will
    be an EXTERNAL/PURGE table.  The location will be omitted from the definition of this table.  Which means
    the data will be copied to the default location for EXTERNAL tables.  That will either be set by
    the metastores `hive.metastoreDirect.warehouse.external.dir` value or the databases definition for `LOCATION`.

    You can use the `-ewd` setting to set the database `LOCATION`, which will be applied to all following
    actions.

    You can optionally use the -f|--flip option to use the RIGHT cluster as the target
    for the DOWNGRADE.

    DOWNGRADE_ACID(Boolean.FALSE),
    */

    /*
    This will transfer the schema only, replace the location with the RIGHT
    clusters location namespace and maintain the relative path.
    The data is transferred by an external process, like 'distcp'.
     */
    SCHEMA_ONLY(Boolean.FALSE, SchemaOnlyDataStrategy.class, "schema-only"),
    /*
    Assumes the clusters are LINKED.  We'll transfer the schema and leave
    the location as is on the new cluster.  This provides a means to test
    Hive on the RIGHT cluster using the LEFT clusters storage.
     */
    LINKED(Boolean.FALSE, LinkedDataStrategy.class, "linked"),
    /*
    Assumes the clusters are LINKED.  We'll use SQL to migrate the data
    from one cluster to another.
     */
    SQL(Boolean.FALSE, SQLDataStrategy.class, "sql"),
    /*
    Assumes the clusters are LINKED.  We'll use EXPORT_IMPORT to get the
    data to the new cluster.  EXPORT to a location on the LEFT cluster
    where the RIGHT cluster can pick it up with IMPORT.
     */
    EXPORT_IMPORT(Boolean.FALSE, ExportImportDataStrategy.class, "export-import"),
    /*
    Hybrid is a strategy that select either SQL or EXPORT_IMPORT for the
    tables data strategy depending on the criteria of the table.
     */
    HYBRID(Boolean.FALSE, HybridDataStrategy.class, "hybrid"),
    /*
    If you had previously created a schema on the RIGHT that is LINKED to the LEFT's data,
    this option provides a way to 'convert' the LINKED schemas to SCHEMA_ONLY without having to
    drop and recreate the schema.  This assumes that the data is migrated through other means.
    The conversion will simple review the RIGHT schema's and convert the NAMESPACE from the LEFT
    to the RIGHT's NAMESPACE.  In addition, tables that were 'legacy' managed and converted to EXTERNAL
    WITHOUT purge, will have the purge flag set since the data is now owned by the RIGHT cluster.
     */
    CONVERT_LINKED(Boolean.FALSE, ConvertLinkedDataStrategy.class, "convert-linked"),
    /*
    Using the LEFT cluster configuration (or RIGHT when `-f|--flip` is used), migrate the tables from
    the current storage location to one of two possibilities: The default locations in 'hms' as identified by
    the metastores `hive.metastoreDirect.warehouse.dir` for MANAGED (ACID) tables and `hive.metastoreDirect.warehouse.external.dir`
    for EXTERNAL tables. As an alternative, the LOCATION and MANAGEDLOCATION properties of the database can be
    configured/changed to a combination of the `-sms`, `-smn|-cs`, `-wd`, and `-ewd`.<p/>

    For ALL:
    We need to alter the database LOCATION and MANAGEDLOCATION properties to a default that relies on the new target
    namespace

    For SCHEMA_ONLY:
    This would be a change to the LOCATION of the 'table' and 'partitions'.  This applies to EXTERNAL tables. For this
    strategy we're assuming the data would be moved via 'distcp'.  See the 'distcp' reports for a shell of what we expect.
    We can NOT move ACID tables this way.

    This option is least desirable because files aren't rewritten (consolidate) and the locations for ALL partitions needs
    to be changed.  Consider removing as option.

    For SQL:
    We'll create 'target' tables rooted on the new namespace and then use SQL to migrate the data from the old table to
    the new table.  We'll rename the old table to an archive before the data is moved.  The new table will have the same
    name as the original.

    For EXPORT_IMPORT:
    Rename Table, EXPORT to export dir.  Create table with original name on the new namespace and IMPORT.

    For HYBRID:
    Choose either EXPORT_IMPORT or SQL.

    We won't automatically DROP old data.  But we will build a script that will drop the tables we rename to facilitate
    this process.

     */
    STORAGE_MIGRATION(Boolean.FALSE, StorageMigrationDataStrategy.class, "storage-migration"),
    /*
    The data storage is shared between the two clusters and no data
    migration is required.  Schema's are transferred using the same
    location.  Commit/ownership?
     */
    COMMON(Boolean.FALSE, CommonDataStrategy.class, "common"),
    ICEBERG_CONVERSION(Boolean.FALSE, IcebergConversionDataStrategy.class, "iceberg-conversion"),
    INTERMEDIATE(Boolean.TRUE, IntermediateDataStrategy.class, "intermediate"),
    SQL_ACID_DOWNGRADE_INPLACE(Boolean.TRUE, SQLAcidInPlaceDataStrategy.class, "sql-acid-downgrade-inplace"),
    EXPORT_IMPORT_ACID_DOWNGRADE_INPLACE(Boolean.TRUE, ExportImportAcidDowngradeInPlaceDataStrategy.class, "export-import-acid-downgrade-inplace"),
    HYBRID_ACID_DOWNGRADE_INPLACE(Boolean.TRUE, HybridAcidDowngradeInPlaceDataStrategy.class, "hybrid-acid-downgrade-inplace"),;

    private final boolean hidden;
    private final Class clazz;
    private final String name;

    DataStrategyEnum(boolean hidden, Class clazz, String name) {
        this.hidden = hidden;
        this.clazz = clazz;
        this.name = name;
    }

    public static DataStrategyEnum[] visibleValues() {
        List<DataStrategyEnum> dsList = new ArrayList<>();
        for (DataStrategyEnum dataStrategy : values()) {
            if (!dataStrategy.hidden) {
                dsList.add(dataStrategy);
            }
        }
        DataStrategyEnum[] rtn = dsList.toArray(new DataStrategyEnum[0]);
        return rtn;
    }

    @SuppressWarnings("unchecked")
    public DataStrategy getDataStrategy() {
        DataStrategy rtn = null;
        try {
            rtn = (DataStrategy) clazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | NoSuchMethodException | InvocationTargetException |
                 IllegalAccessException e) {
            log.error(e.getMessage(), e);
        }
        return rtn;
    }
}
