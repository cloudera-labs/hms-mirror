# Storage Migration

## Location Translation Strategy

```yaml
transfer:
  storageMigration:
    translationType: "ALIGNED|RELATIVE"
```

## Data Movement Strategy

```yaml
transfer:
  storageMigration:
    dataMovementStrategy: "SQL|DISTCP"
```

## Skip Database Location Adjustments

When true, the database location will NOT be adjusted to match the table locations that are migrated. " +
"This is useful in the case of an 'archive' strategy where you only want to migrate the table(s) but are not yet " +
"ready to migration or stage future tables at the new location.

```yaml
transfer:
  storageMigration:
    skipDatabaseLocationAdjustments: true|false
```

## Create Archive

When true (default is false), the tables being migrated will be archived vs. simply changing the location details " +
"of the tables metadata.  This is relevant only for STORAGE_MIGRATION when using the 'DISTCP' data movement " +
"strategy. When the data movement strategy is 'SQL' for STORAGE_MIGRATION, this flag is ignored because the default " +
"behavior is to create an archive table anyhow.
    
```yaml
transfer:
  storageMigration:
    createArchive: true|false
```

## Consolidate Source Tables

Used to help define how a `distcp` plan will be built when asked for.  The default is `false` which means that will 
**NOT** be consolidating table locations.  

If this is set to `true`, the `distcp` plan will **remove the table location** from the source (which is generally the 
database location) and use it for all transfers.  This looks more simple, but could lead to copying more data than
you expect since there's no guarantee that there isn't a lot of other 'extra' data in the source location.

```yaml
transfer:
  storageMigration:
    consolidateTablesForDistcp: true|false
```