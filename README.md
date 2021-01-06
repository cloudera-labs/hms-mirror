# HMS-Mirror

"hms-mirror" is a utility used to bridge the gap between two clusters and migrate `hive` _metadata_ **AND** _data_.

1. [Getting / Building](./setup.md)
2. Pre-Requisites
   1. [Shared Auth Model](./shared_auth.md)
   1. [Link Clusters](./link_clusters.md) 
   1. [Permissions](./permissions.md)
2. [Configuration](./configuration.md)   
2. [Running HMS Mirror](./running.md)
3. [Must know run tips/Do NOT SKIP](./running_tips.md)   
4. [Logs and Reports](./logs_reports.md)   
3. [Processes](#processes)
   1. [Metadata](./metadata.md)
   1. [Storage](./storage.md)
4. [Optimizations](./optimizations.md)
5. [Troubleshooting](./troubleshooting.md)   
4. [Design-Premise](./design-spec.md)

## Processes

There are 2 process catagories in `hms-mirror`.  They can be used in isolation.  Migrated schemas go through a set of rules that convert their definition to with Hive 3, while maintaining as much legacy behaviors as possible.  These include:
- Managed non-acid tables are converted to _EXTERNAL_ tables.

[Metadata](./metadata.md)

[Storage](./storage.md)







