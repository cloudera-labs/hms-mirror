# Configuration

The configuration is done via a 'yaml' file, details below.

There are two ways to get started:
- The first time you run `hms-mirror` and it can't find a configuration, it will walk you through building one and save it to `$HOME/.hms-mirror/cfg/default.yaml`.  Here's what you'll need to complete the setup:
    - URI's for each clusters HiveServer2
    - STANDALONE jar files for EACH Hive version.
        - We support the Apache Hive based drivers for Hive 1 and Hive2/3.
        - Recently added support for the Cloudera JDBC driver for CDP.
    - Username and Password for non-kerberized connections.
        - Note: `hms-mirror` will only support one kerberos connection.  For the other, use another AUTH method.
    - The hcfs (Hadoop Compatible FileSystem) protocol and prefix used for the hive table locations in EACH cluster.
- Use the [template yaml](hms-mirror-Default-Configuration-Template.md) for reference and create a `default.yaml` in the running users `$HOME/.hms-mirror/cfg` directory.

You'll need JDBC driver jar files that are **specific* to the clusters you'll integrate.  If the **LEFT** cluster isn't the same version as the **RIGHT** cluster, don't use the same JDBC jar file, especially when integrating Hive 1 and Hive 3 services.  The Hive 3 driver is NOT backwardly compatible with Hive 1.

See the [running](hms-mirror-running.md) section for examples on running `hms-mirror` for various environment types and connections.

## JDBC Drivers and Configuration

`hms-mirror` requires JDBC drivers to connect to the various end-points needed to perform it's tasks.  The `LEFT` and `RIGHT` cluster endpoints for HiveServer2 require the standalone JDBC drivers that are specific to that Hive version.

`hms-mirror` supports the Apache and packaged hive **standalone** drivers that are found with your distribution.  For CDP, we also support to Cloudera JDBC driver found and maintained at on the [Cloudera Hive JDBC Downloads Page](https://www.cloudera.com/downloads/connectors/hive/jdbc).  Note that the URL configurations between the Apache and Cloudera JDBC drivers are different.

When using the Cloudera JDBC driver, you'll need to add the property `driverClassName: "com.cloudera.hive.jdbc.HS2Driver"` to the `hiveServer2` configuration. If you're NOT using the Cloudera JDBC driver, just remove the property.

```yaml
hiveServer2:
  uri: "<cloudera_jdbc_url>"
  driverClassName: "com.cloudera.hive.jdbc.HS2Driver"
  connectionProperties:
    user: "xxx"
    password: "xxx"
```

Starting with the Apache Standalone driver shipped with CDP 7.1.8 cummulative hot fix parcels, you will need to include additional jars in the configuration `jarFile` configuration, due to some packaging adjustments.

For example: `jarFile: "<cdp_parcel_jars>/hive-jdbc-3.1.3000.7.1.8.28-1-standalone.jar:<cdp_parcel_jars>/log4j-1.2-api-2.18.0.jar:<cdp_parcel_jars>/log4j-api-2.18.0.jar:<cdp_parcel_jars>/log4j-core-2.18.0.jar"` NOTE: The jar file with the Hive Driver MUST be the first in the list of jar files.

The Cloudera JDBC driver shouldn't require additional jars.

#### Example URLs

##### **CDP Hive via Knox Gateway**

Doesn't require Kerberos.  Knox is SSL, so depending on whether you've self-signed your certs you may need to make adjustments.

- Apache Hive and CDP Packaged Apache Hive JDBC Driver
> `jdbc:hive2://s03.streever.local:8443/;ssl=true;transportMode=http;httpPath=gateway/cdp-proxy-api/hive;sslTrustStore=/Users/dstreev/bin/certs/gateway-client-trust.jks;trustStorePassword=changeit`

- Cloudera JDBC Driver
> `jdbc:hive2://s03.streever.local:8443;transportMode=http;AuthMech=3;httpPath=gateway/cdp-proxy-api/hive;SSL=1;AllowSelfSignedCerts=1`

##### **CDP Hive direct with Kerberos**

When connecting to via Kerberos, place the JDBC driver for Hive in the `$HOME/.hms-mirror/aux_libs` directory. This ensures it is loaded in the classpath correctly to support the kerberos connection.  If you're NOT using Kerberos connection, the libraries should be reference in the `jarFile` parameter and NOT be in the `aux_libs` directory.

If you are experimenting with different connection types, ensure the jar file is REMOVED from `aux_libs` when trying other configurations.

- Apache JDBC Driver (packaged with CDP)
> `tbd`
> NOTE: Our experience with this driver is that requires the use of `--hadoop-classpath` in the commandline to load the needed kerberos libraries.


- Cloudera JDBC Driver
> `jdbc:hive2://s04.streever.local:10001;transportMode=http;AuthMech=1;KrbRealm=STREEVER.LOCAL;KrbHostFQDN=s04.streever.local;KrbServiceName=hive;KrbAuthType=2;httpPath=cliservice;SSL=1;AllowSelfSignedCerts=1`
> NOTE: When using this driver, our experience has been that you do NOT need to use `--hadoop-classpath` as a commandline element with versions 1.6.1.0+

##### **HDP2 HS2 with No Auth**

Since CDP is usually kerberized AND `hms-mirror` doesn't support the simultanous connections to 2 different kerberos environments, I've setup an HS2 on HDP2 specifically for this effort. NOTE: You need to specify a `username` when connecting to let Hive know what the user is.  No password required.

- Apache Hive Standalone Driver shipped with HDP2.
> `jdbc:hive2://k02.streever.local:10000`


### Direct Metastore DB Acceess for `-epl`

The `LEFT` and `RIGHT` configurations also suppport 'direct' metastore access to collect detailed partition information when using the flag `-epl`.  The support this feature, get the JDBC driver that is appropriate for your metastore(s) backend dbs and place it in `$HOME/.hms-mirror/aux_libs` directory.


## Secure Passwords in Configuration

There are two passwords stored in the configuration file mentioned above.  One for each 'JDBC connection, if those rely on a password for connect.  By default, the passwords are in clear text in the configuration file.  This usually isn't an issue since the file can be protected at the UNIX level from peering eyes.  But if you need to protect those passwords, `hms-mirror` supports storing an encrypted version of the password in the configuration.

The `password` element for each JDBC connection can be replaced with an **encrypted** version of the password and read by `hms-mirror` during execution, so the clear text version of the password isn't persisted anywhere.

When you're using this feature, you need to have a `password-key`.  This is a key used to encrypt and decrypt the password in the configuration.  The same `password-key` must be used for each password in the configuration file.

### Generate the Encrypted Password

Use the `-pkey` and `-p` options of `hms-mirror`

`hms-mirror -pkey cloudera -p have-a-nice-day`

Will generate:
```
...
Encrypted password: HD1eNF8NMFahA2smLM9c4g==
```

Copy this encrypted password and place it in your configuration file for the JDBC connection.  Repeat for the other passwords, if it's different, and paste it in the configuration as well.

### Running `hms-mirror` with Encrypted Passwords

Using the **same** `-pkey` you used to generate the encrypted password, we'll run `hms-mirror`

`hms-mirror -db <db> -pkey cloudera ...`

When the `-pkey` option is specified **WITHOUT** the `-p` option (used previously), `hms-mirror` will understand to **decrypt** the configuration passwords before connecting to jdbc.  If you receive jdbc connection exceptions, recheck the `-pkey` and encrypted password from before.
