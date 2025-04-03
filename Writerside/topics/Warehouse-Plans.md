# Warehouse Plans

Warehouse Plans in "hms-mirror" are a database-level configuration mechanism designed to manage and map storage locations within a Hive database during metadata migration. They involve reviewing the database metadata to identify all storage locations (e.g., table and partition locations for both External and Managed tables) and mapping these to predefined Warehouse Plan locations. The intersection of this metadata and the Warehouse Plan is then used to construct **Global Location Maps**, which "hms-mirror" uses internally to provide a consistent mapping between storage systems across clusters or within a single cluster.

There are three types of 'Warehouse Plans'.

- Global
- Per Database
- Environment

When you choose to 'ALIGN' your datasets during the migration (See [Location Alignment](Location-Alignment.md) for
settings that are applicable for each strategy), we'll evaluate the warehouse plans to determine where each dataset
should land.  If you're using 'SQL' to move data, we'll build the schema's and sql that will make the adjustments
required to reorganize the data based on the warehouse plan that is found.

It's recommended to define a warehouse plan for each **database** you want to move when you're using the 'ALIGNED'
data movement strategy.  When this is defined for the database, we'll inspect the all the current locations of tables
and partitions in that dataset and make the necessary adjustments to locations.

The 'Warehouse Plans' get converted into 'Global Location Maps' that are inspected during processing to make that conversion.

## Standard Locations

When you choose to **ALIGN** the datasets, you are choosing to collect everything in the dataset/database under the same
location as you've defined in the **warehouse plan**.  When you choose `DISTCP` as the movement plan for `ALIGNED`, we'll
build a distcp plan that will make those translations.  **BUT**, there are some restrictions to this.

When you choose to use non-standard locations for 'partition specs', we can't build a proper `distcp` plan.  In this case
we will throw an 'error' for the offending table and describe to imbalance.  You can either fix/adjust the dataset OR choose
to use the `SQL` data movement strategy.

## Warehouse Plans Explained

### What Are Warehouse Plans For?

- **Purpose**: At the database level, Warehouse Plans define how storage locations in the source database metadata are translated to target locations. This process ensures that the migrated metadata aligns with the desired storage structure on the target system. Unlike the earlier [`globalLocationMap`](Release-Notes.md#global-location-maps), which offered a simpler, global key-value mapping, Warehouse Plans operate specifically at the database scope, providing a more targeted and detailed approach.
- [**Database Warehouse Plans**](Database-Warehouse-Plans.md) : These are the core of the feature, specifying location mappings for a single database. The documentation implies that ["Global Warehouse Plans"](Global-Warehouse-Plans.md) may extend this concept across multiple databases, but the primary focus is on individual database-level planning.

The resulting Global Location Maps, derived from Warehouse Plans, serve as an internal framework for "hms-mirror" to handle location translations during migration, ensuring consistency between source and target storage systems.

### What to Use Warehouse Plans For?

Warehouse Plans are primarily designed for scenarios where storage locations within a database need to be systematically mapped or reorganized during a migration. Here’s an explanation of their use cases, reflecting their database-level scope and original intent for `STORAGE_MIGRATION`, as well as their broader applicability:

1. **Primary Use Case: Storage Migration Within a Cluster (STORAGE_MIGRATION)**:
    - **Scenario**: You’re migrating the storage layer behind a database’s metadata from HDFS to Ozone (or another system like an encrypted zone), as highlighted in the [`STORAGE_MIGRATION` strategy](storage_migration.md).
    - **Use**: Define a Warehouse Plan for the database to map all existing locations (e.g., `/apps/hive/warehouse/my_db`) to a new target (e.g., `ofs://ozone1/vol1/bucket1/my_db`). The metadata is reviewed, and all table and partition locations are updated accordingly. The resulting Global Location Maps ensure the migration reflects the new storage system.
    - **Example**: For a database `finance_db`:
      ```yaml
      databaseWarehousePlans:
        finance_db:
          EXTERNAL_TABLE: "/finance_db/ext"
          MANAGED_TABLE: "/finance_db/managed"
      ```
      This maps all External and Managed table locations within `finance_db` to the specified Ozone paths.
    - The Ozone Namespace is pulled from the `targetNamespace` which can be set in the config or via the Web UI.
      ```yaml
      transfer:
        targetNamespace: ofs://myOzone
      ```

2. **Reorganizing Storage During Schema Migration (SCHEMA_ONLY)**:
    - **Scenario**: You’re migrating a database’s schema between clusters (e.g., on-premises to cloud) using `SCHEMA_ONLY` (page 150), and you want to reorganize storage locations simultaneously.
    - **Use**: A Warehouse Plan can redefine the database’s storage locations (e.g., from `/data/my_db` to `s3a://mybucket/my_db`). Since `SCHEMA_ONLY` doesn’t move data, pair it with the [`-dc|--distcp` option](cli-options.md) to generate `distcp` plans for separately migrating the data to the new locations.
    - **Example**:
      ```yaml
      databaseWarehousePlans:
        my_db:
          EXTERNAL_TABLE: "/my_db/ext"
          MANAGED_TABLE: "/my_db/managed"
      ```
      Run: `hms-mirror -d SCHEMA_ONLY -db my_db -dc` to get the schema migration and a `distcp` plan. Again, use the `transfer.targetNamespace` to set the `s3` location. EG `s3a://mybucket`.

3. **Migrating and Moving Data with SQL Strategy (SQL)**:
    - **Scenario**: You’re using the [`SQL` data strategy](SQL.md) to migrate both metadata and data between linked clusters, and you need to adjust storage locations.
    - **Use**: Define a Warehouse Plan to map the database’s locations to the target cluster’s storage (e.g., from `hdfs://source/my_db` to `hdfs://target/my_db`). The SQL strategy will move the data to the new locations as part of the migration.
    - **Example**:
      ```yaml
      databaseWarehousePlans:
        my_db:
          EXTERNAL_TABLE: "/my_db/ext"
          MANAGED_TABLE: "/my_db/managed"
      ```
      Run: `hms-mirror -d SQL -db my_db`.

4. **Supporting Data Strategies Without Data Movement**:
    - **Scenario**: You’re using a strategy like [`SCHEMA_ONLY`](SCHEMA_ONLY.md) that doesn’t move data, but you need a plan to migrate the data later.
    - **Use**: Warehouse Plans map the database’s locations, and the `-dc|--distcp` option generates `distcp` scripts to handle the data migration separately, aligning with the mapped locations.
    - **Example**: For `SCHEMA_ONLY`:
      ```bash
      hms-mirror -d SCHMEA_ONLY -db my_db -dc -wps my_db=/my_db/ext:/my_db/managed
      ```
      The Warehouse Plan ensures the dump reflects the intended target locations, and `distcp` plans are provided.

5. **Consistency Within a Database Across Table Types**:
    - **Scenario**: A database contains both External and Managed tables with various locations, and you want a unified storage structure on the target.
    - **Use**: A Warehouse Plan reviews all locations in the database metadata and maps them to consistent target locations, regardless of table type. This is more precise than global mappings, as it’s tailored to the database.
    - **Example**: Mapping all tables in `sales_db` to a single storage layer:
      ```yaml
      databaseWarehousePlans:
        sales_db:
          EXTERNAL_TABLE: "/new_storage/division_ext"
          MANAGED_TABLE: "/new_storage/division_mngd"
      ```

### How to Use Warehouse Plans

To implement Warehouse Plans, configure them in the `default.yaml` file under the `databaseWarehousePlans` section at the database level.

Here’s how, based on the documentation and your clarification:

- **Syntax**:
  ```yaml
  databaseWarehousePlans:
    <database_name>:
      EXTERNAL_TABLE: "<target_location_for_external>"
      MANAGED_TABLE: "<target_location_for_managed>"
  ```
    - The metadata for `<database_name>` is analyzed, and all locations (for External and Managed tables) are mapped to the specified targets. These mappings feed into the Global Location Maps used internally.
    - NOTE: The database name is appended to the location you specify, so do NOT include that in the location.  This allows you to use the same location for multiple databases in the scenario you want to have multiple databases share the same root location.

- **Command-Line Integration**: While Warehouse Plans are configuration-driven, they work with strategies like `STORAGE_MIGRATION`, `SCHEMA_ONLY`, or `SQL`. Use `-dc|--distcp` for strategies without data movement.
    - The Warehouse Plans can be set in the config, via the Web UI, or via the commandline option `-wps <db=ext-dir:mngd-dir[,db=ext-dir:mngd-dir]...>`

  ```
  hms-mirror -d STORAGE_MIGRATION -db my_db -dc -wps my_db=/my_db/ext:/my_db/managed
  ```
  or
  ```
  hms-mirror -d SCHEMA_ONLY -db my_db -dc
  ```

- **Execution**: The Warehouse Plan drives the location translation process. For `STORAGE_MIGRATION`, data is moved directly (if using `DISTCP` or `SQL` as the data movement strategy, page 202). For other strategies, `-dc` ensures data migration plans are provided.

### Practical Tips
- **Database Scope**: Define Warehouse Plans per database to ensure precise mapping. Unlike the older `globalLocationMap`, they don’t apply globally unless explicitly extended via "Global Warehouse Plans."
- **Dry-Run**: Test with a dry-run (`hms-mirror -db <db_name>`) to review the mappings in the output reports (page 111) before executing (`-e`).
- **Storage Access**: Verify permissions for the mapped locations, especially for cross-cluster scenarios (page 69, "Linking Cluster Storage Layers").
- **Original Intent**: While designed for `STORAGE_MIGRATION` (e.g., HDFS to Ozone), their flexibility supports broader reorganization tasks.

In summary, Warehouse Plans are a database-level tool in "hms-mirror" for mapping storage locations within a database’s metadata, originally for `STORAGE_MIGRATION` (e.g., HDFS to Ozone), but also valuable for `SCHEMA_ONLY` and `SQL` strategies. They construct Global Location Maps internally, ensuring accurate location translations, and can be paired with `-dc|--distcp` for separate data movement when needed.
