# Environment Warehouse

When a job is started, we'll gather details about the environment from Hive via `set;`. Hive defines default global
warehouse locations for 'external' and 'managed' tables for databases. If a database doesn't define a `LOCATION`
and/or `MANAGEDLOCATION`, these entries will be used to define to locations of a table/partition when it's created.

In a multi-tenant environment where different storage locations and/or namespaces are used, these global default settings
are inadequate.  In these environments, you should be declaring the locations at the database level to ensure all new datasets
end up in the desired locations and you can maintain a true multi-tenant environment.