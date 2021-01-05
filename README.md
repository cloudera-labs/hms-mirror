# HMS-Mirror

"hms-mirror" is a utility used to bridge the gap between two clusters and migrate `hive` _metadata_ **AND** _data_.

1. [Getting / Building](./setup.md)
1. [Link Clusters](./link_clusters.md)   
2. [Configuration](./configuration.md)   
2. [Running HMS Mirror](./running.md)
3. [Processes](#processes)
   1. [Metadata](./metadata.md)
   1. [Storage](./storage.md)
4. [Tips and Optimizations](./optimizations.md)
5. [Troubleshooting](./troubleshooting.md)   
4. [Design-Premise](./design-spec.md)

## Processes

There are 2 process catagories in `hms-mirror`.  They can be used in isolation.  Migrated schemas go through a set of rules that convert their definition to with Hive 3, while maintaining as much legacy behaviors as possible.  These include:
- Managed non-acid tables are converted to _EXTERNAL_ tables.

[Metadata](./metadata.md)

[Storage](./storage.md)






