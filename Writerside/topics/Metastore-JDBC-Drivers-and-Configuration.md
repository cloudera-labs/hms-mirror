# Metastore JDBC Drivers and Configuration

For some data points, we revert to direct access to the metastore RDBMS.  To support this, we need to include the 
JDBC drivers for the metastore RDBMS in the `aux_libs` directory.  The `aux_libs` directory is located in the 
`$HOME/.hms-mirror` directory.  The JDBC drivers should be placed in the `aux_libs` directory and the configuration 
setup for the session.

<tabs>
<tab id="web-msd" title="Web UI">

![metastore_direct_cfg.png](metastore_direct_cfg.png)

</tab>
<tab id="cli-msd" title="CLI">

</tab>
</tabs>