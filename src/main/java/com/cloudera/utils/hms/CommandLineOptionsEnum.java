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

package com.cloudera.utils.hms;

import lombok.Getter;

@Getter
public enum CommandLineOptionsEnum {
    ACID_PARTITION_COUNT("ap", "acid-partition-count", "count", ""),
    AVRO_SCHEMA_MIGRATION("asm", "avro-schema-migration", null, ""),
    AUTO_TUNE("at", "auto-tune", null, ""),
    CFG("cfg", "config", "filename", ""),
    CREATE_IF_NOT_EXIST("cine", "create-if-not-exist", null, ""),
    COMMON_STORAGE("cs", "common-storage", "storage-path", ""),
    COMPRESS_TEXT_OUTPUT("cto", "compress-text-output", null, ""),
    DATA_STATEGY("d", "data-strategy", "strategy", ""),
    DOWNGRADE_ACID("da", "downgrade-acid", null, ""),
    DATABASE("db", "database", "databases", ""),
    DATABASE_ONLY("dbo", "database-only", null, ""),
    DATABASE_PREFIX("dbp", "db-prefix", "prefix", ""),
    DATABASE_RENAME("dbr", "db-rename", "rename", ""),
    DATABASE_REGEX("dbRegEx", "database-regex", "regex", ""),
    //TODO: Double check conversion from legacy.
    DISTCP("dc", "distcp", "flow-direction default:PULL", ""),
    DECRYPT_PASSWORD("dp", "decrypt-password", "encrypted-password", ""),
    DUMP_SOURCE("ds", "dump-source", "source", ""),
    DUMP_TEST_DATA("dtd", "dump-test-data", null, ""),
    EXECUTE("e", "execute", null, ""),
    EXPORT_PARTITION_COUNT("ep", "export-partition-count", "limit", ""),
    EVALUATE_PARTITION_LOCATION("epl", "evaluate-partition-location", null, ""),
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
    SORT_DYNAMIC_PARTITION_INSERTS("sdpi", "sort-dynamic-partition-inserts", null, ""),
    SKIP_FEATURES("sf", "skip-features", null, ""),
    SKIP_LINK_CHECK("slc", "skip-link-check", null, ""),
    SKIP_LEGACY_TRANSLATION("slt", "skip-legacy-translation", null, ""),
    STORAGE_MIGRATION_NAMESPACE("smn", "storage-migration-namespace", "namespace", ""),
    SKIP_OPTIMIZATIONS("so", "skip-optimizations", null, ""),
    SQL_PARTITION_COUNT("sp", "sql-partition-count", "limit", ""),
    SQL_OUTPUT("sql", "sql-output", null, ""),
    SKIP_STATS_COLLECTION("ssc", "skip-stats-collection", null, ""),
    SETUP("su", "setup", null, ""),
    TABLE_EXCLUDE_FILTER("tef", "table-exclude-filter", "regex", ""),
    TABLE_FILTER("tf", "table-filter", "regex", ""),
    TABLE_FILTER_PARTITION_COUNT_LIMIT("tfp", "table-filter-partition-count-limit", "partition-count", ""),
    TABLE_FILTER_SIZE_LIMIT("tfs", "table-filter-size-limit", "size MB", ""),
    TRANSFER_OWNERSHIP("to", "transfer-ownership", null, ""),
    VIEW_ONLY("v", "views-only", null, ""),
    WAREHOUSE_DIRECTORY("wd", "warehouse-directory", "path", ""),
    UI("wi", "web-interface", null, "");

    private final String shortName;
    private final String longName;
    private final String argumentName;
    private final String description;

    CommandLineOptionsEnum(String shortName, String longName, String argumentName, String description) {
        this.shortName = shortName;
        this.longName = longName;
        this.argumentName = argumentName;
        this.description = description;
    }
}
