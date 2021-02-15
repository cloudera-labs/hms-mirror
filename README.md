# HMS-Mirror

"hms-mirror" is a utility used to bridge the gap between two clusters and migrate `hive` _metadata_ **AND** _data_.

1. [Getting / Building](./setup.md)
2. [Optimizations](./optimizations.md)
3. Pre-Requisites
   1. [Protect yourself!  Backups!](./backups.md)
   1. [Shared Auth Model](./shared_auth.md)
   1. [Link Clusters](./link_clusters.md) 
   1. [Permissions](./permissions.md)
4. [Configuration](./configuration.md)   
5. [RUN tips/Do NOT SKIP](./running_tips.md)
6. [Running HMS Mirror](./running.md)
7. [Output Reports and Logs](./logs_reports.md)   
8. [Strategies](#Strategies)
   1. [Schema Only](./schema_only.md)
   1. [Linked](./linked.md)
   1. [SQL](./sql.md)
   1. [EXPORT_IMPORT](./export_import.md)
   1. [HYBRID](./hybrid.md)
   1. [Intermediate](./intermediate.md)
   1. [Common](./common.md)
9. [Troubleshooting](./troubleshooting.md)   
10. [Design-Premise](./design-spec.md)

## Strategies

There are 2 process catagories in `hms-mirror`.  They can be used in isolation.  Migrated schemas go through a set of rules that convert their definition to with Hive 3, while maintaining as much legacy behaviors as possible.  These include:
- Managed non-acid tables are converted to _EXTERNAL_ tables.
  
[Schema Only](./schema_only.md)

[Linked](./linked.md)

[SQL](./sql.md)

[EXPORT_IMPORT](./export_import.md)

[HYBRID](./hybrid.md)

[Intermediate](./intermediate.md)

[Common](./common.md)







