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

package com.cloudera.utils.hms.mirror.cli;

import lombok.Getter;

@Getter
public enum HmsMirrorCommandLineOptionsEnum {
    ACID_PARTITION_COUNT("ap", "acid-partition-count", "count", ""),
    AVRO_SCHEMA_MIGRATION("asm", "avro-schema-migration", null, ""),
    AUTO_TUNE("at", "auto-tune", null, ""),
    BETA("b", "beta", null, ""),
    CFG("cfg", "config", "filename", ""),
    COMMENT("com", "comment", null, "Comments to add to report output."),
    CONCURRENCY("c", "concurrency", "threads", ""),
    CONSOLIDATE_TABLES_FOR_DISTCP("ctfd", "consolidate-tables-for-distcp", null, ""),
    CREATE_IF_NOT_EXIST("cine", "create-if-not-exist", null, ""),
    TARGET_NAMESPACE("tns", "target-namespace", "target", ""),
    COMPRESS_TEXT_OUTPUT("cto", "compress-text-output", null, ""),
    DATA_STATEGY("d", "data-strategy", "strategy", ""),
    DOWNGRADE_ACID("da", "downgrade-acid", null, ""),
    DATABASE("db", "database", "databases", ""),
    DATABASE_ONLY("dbo", "database-only", null, ""),
    DATABASE_PREFIX("dbp", "db-prefix", "prefix", ""),
    DATABASE_SKIP_PROPERTIES("dbsp", "database-skip-properties", "key(s)", ""),
    DATABASE_RENAME("dbr", "db-rename", "rename", ""),
    DATABASE_REGEX("dbRegEx", "database-regex", "regex", ""),
    //TODO: Double check conversion from legacy.
    DISTCP("dc", "distcp", "flow-direction default:PULL", ""),
    DATA_MOVEMENT_STRATEGY("dms", "data-movement-strategy", "strategy", ""),
    DECRYPT_PASSWORD("dp", "decrypt-password", "encrypted-password", ""),
    DUMP_SOURCE("ds", "dump-source", "source", ""),
    DUMP_TEST_DATA("dtd", "dump-test-data", null, ""),
    EXECUTE("e", "execute", null, ""),
    EXPORT_PARTITION_COUNT("ep", "export-partition-count", "limit", ""),
    ALIGN_LOCATIONS_WITH_DB("al", "align-locations", null, ""),
    EXTERNAL_WAREHOUSE_DIRECTORY("ewd", "external-warehouse-directory", "path", ""),
    FLIP("f", "flip", null, ""),
    FORCE_EXTERNAL_LOCATION("fel", "force-external-location", null, ""),
    GLOBAL_LOCATION_MAP("glm", "global-location-map", "key=value", ""),
    HELP("h", "help", null, ""),
    IN_PLACE("ip", "in-place", null, ""),
    INTERMEDIATE_STORAGE("is", "intermediate-storage", "storage-path", ""),
    ICEBERG_TABLE_PROPERTY_OVERRIDES("itpo", "iceberg-table-property-overrides", "key=value", ""),
    ICEBERG_VERSION("iv", "iceberg-version", "version", ""),
    LOAD_TEST_DATA("ltd", "load-test-data", "file", ""),
    MIGRATE_ACID("ma", "migrate-acid", "bucket-threshold (2)", ""),
    MIGRATE_ACID_ONLY("mao", "migrate-acid-only", "bucket-threshold (2)", ""),
    MIGRATE_NON_NATIVE("mnn", "migrate-non-native", null, ""),
    MIGRATE_NON_NATIVE_ONLY("mnno", "migrate-non-native-only", null, ""),
    NO_PURGE("np", "no-purge", null, ""),
    OUTPUT_DIRECTORY("o", "output-dir", "outputdir", ""),
    PASS_THROUGH("pt", "pass-through", "spring-setting", ""),
    PASSWORD("p", "password", "password", ""),
    PASSWORD_KEY("pkey", "password-key", "password-key", ""),
    PROPERTY_OVERRIDES("po", "property-overrides", "key=value", ""),
    PROPERTY_OVERRIDES_LEFT("pol", "property-overrides-left", "key=value", ""),
    PROPERTY_OVERRIDE_RIGHT("por", "property-overrides-right", "key=value", ""),
    QUIET("q", "quiet", null, ""),
    RESET_TO_DEFAULT_LOCATION("rdl", "reset-to-default-location", null, ""),
    REPLAY("replay", "replay", "replay-directory", ""),
    RIGHT_IS_DISCONNECTED("rid", "right-is-disconnected", null, ""),
    READ_ONLY("ro", "read-only", null, ""),
    RESET_RIGHT("rr", "reset-right", null, ""),
    SYNC("s", "sync", null, ""),
    SAVE_WORKING_TABLES("swt", "save-working-tables", null, ""),
    SORT_DYNAMIC_PARTITION_INSERTS("sdpi", "sort-dynamic-partition-inserts", null, ""),
    SKIP_FEATURES("sf", "skip-features", null, ""),
    SKIP_LINK_CHECK("slc", "skip-link-check", null, ""),
    SKIP_LEGACY_TRANSLATION("slt", "skip-legacy-translation", null, ""),
    STORAGE_MIGRATION_NAMESPACE("smn", "storage-migration-namespace", "namespace", ""),
    STORAGE_MIGRATION_STRICT("sms", "storage-migration-strict", null, ""),
    SKIP_OPTIMIZATIONS("so", "skip-optimizations", null, ""),
    SQL_PARTITION_COUNT("sp", "sql-partition-count", "limit", ""),
    SQL_OUTPUT("sql", "sql-output", null, ""),
    SKIP_STATS_COLLECTION("ssc", "skip-stats-collection", null, ""),
    SETUP("su", "setup", null, ""),
    SUPPRESS_WARNINGS("scw", "suppress-cli-warnings", null, ""),
    TABLE_EXCLUDE_FILTER("tef", "table-exclude-filter", "regex", ""),
    TABLE_FILTER("tf", "table-filter", "regex", ""),
    TABLE_FILTER_PARTITION_COUNT_LIMIT("tfp", "table-filter-partition-count-limit", "partition-count", ""),
    TABLE_FILTER_SIZE_LIMIT("tfs", "table-filter-size-limit", "size MB", ""),
    TRANSFER_OWNERSHIP("to", "transfer-ownership", null, ""),
    TRANSFER_OWNERSHIP_DATABASE("todb", "transfer-ownership-database", null, ""),
    TRANSFER_OWNERSHIP_TABLE("totbl", "transfer-ownership-table", null, ""),
    TRANSLATION_TYPE("tt", "translation-type", "type", ""),
    VIEW_ONLY("v", "views-only", null, ""),
    WAREHOUSE_DIRECTORY("wd", "warehouse-directory", "path", ""),
    WAREHOUSE_PLANS("wps", "warehouse-plans", "db=ext-dir:mngd-dir", "");
    
    private final String shortName;
    private final String longName;
    private final String argumentName;
    private final String description;

    HmsMirrorCommandLineOptionsEnum(String shortName, String longName, String argumentName, String description) {
        this.shortName = shortName;
        this.longName = longName;
        this.argumentName = argumentName;
        this.description = description;
    }
}
