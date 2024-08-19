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