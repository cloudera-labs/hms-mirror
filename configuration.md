## Configuration

The configuration is done via a 'yaml' file, details below.

Use the [template yaml](./configs/default.template.yaml) for reference and create a `default.yaml` in the running users `$HOME/.hms-mirror/cfg` directory.

You'll need jdbc driver jar files that are **specific* to the clusters you'll integrate with.  If the **LOWER** cluster isn't the same version as the **UPPER** cluster, don't use the same jdbc jar file especially when integrating Hive 1 and Hive 3 services.  The Hive 3 driver is NOT backwardly compatible with Hive 1.

### [Definitions and Template](./configs/default.template.yaml)
