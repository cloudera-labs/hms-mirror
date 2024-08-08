# Databases

The whole goal of `hms-mirror` is to move metadata from one cluster to another. Picking which databases to move is
a primary function of the application.

There are several ways to select databases for migration. Each with its own benefits.

> Once a method is selected to add database(s), the other add options will be limited until the option is cleared.

### Add 'database' names to the runtime configuration.

This is the simplest way to select databases. Additional filtered can be applied to tables through the table
**RegEx** filters.

<tabs>
<tab title="Web UI">
Ensure you've selected 'Edit' from the Left Navigation Menu.  Enter a comma separated list of databases and press 'Add'.

![dbs_by_comma.png](dbs_by_comma.png)

</tab>
<tab title="CLI">
The `-db|--database` option allows you to list the databases you want to 
process.
</tab>
</tabs>

### Use the `Database RegEx` filter to include matching databases found in the source cluster.

<tabs>
<tab title="Web UI">
Ensure you've selected 'Edit' from the Left Navigation Menu.  Enter a RegEx pattern to match the databases you want to include and press 'Add'.
</tab>
<tab title="CLI">
The `-dbRegEx|--database-regex` option allows you to filter matching databases.
</tab>
</tabs>

### Create Warehouse Plans for the databases you want to include.

<tabs>
<tab title="Web UI">
Ensure you've selected 'Edit' from the Left Navigation Menu.  Select the 'Warehouse Plans' tab.  Create a new Warehouse Plan and add the databases you want to include.
</tab>
<tab title="CLI">
This feature isn't available through CLI commandline Options. It is possible to use the WebUI to create the 
configuration and then use the 'persisted' version of that configuration as the configuration for the CLI via `-cfg`.
</tab>
</tabs>