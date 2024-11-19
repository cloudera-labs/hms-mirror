# Concurrency

This controls how many parallel operations can be performed at once.  The default is `10` and can be overridden during application startup.

The 'concurrency' setting was previously in the configuration file, but is now only set at startup.  The setting in the configuration file will be ignored.

<tabs>
<tab id="WebUI" title="WebUI">

To adjust the concurrency setting for the Web Service, add `hms-mirror.concurrency.max-threads=n` to the startup command.

`hms-mirror --service --hms-mirror.concurrency.max-threads=n`

</tab>
<tab id="cli" title="CLI">

To adjust the concurrency setting, use the `-c|--concurrency` option when starting the application.
</tab>
</tabs>


This setting dictates the number of connections made to the various endpoints like both Hive Server 2's (LEFT and 
RIGHT) and the Metastore Direct connections.  

> These services need to be able to support these connections.
