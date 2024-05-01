# Example URL&apos;s

## **CDP Hive via Knox Gateway**

Doesn't require Kerberos.  Knox is SSL, so depending on whether you've self-signed your certs you may need to make adjustments.

- Apache Hive and CDP Packaged Apache Hive JDBC Driver
> `jdbc:hive2://s03.streever.local:8443/;ssl=true;transportMode=http;httpPath=gateway/cdp-proxy-api/hive;sslTrustStore=/Users/dstreev/bin/certs/gateway-client-trust.jks;trustStorePassword=changeit`

- Cloudera JDBC Driver
> `jdbc:hive2://s03.streever.local:8443;transportMode=http;AuthMech=3;httpPath=gateway/cdp-proxy-api/hive;SSL=1;AllowSelfSignedCerts=1`

## **CDP Hive direct with Kerberos**

When connecting to via Kerberos, place the JDBC driver for Hive in the `$HOME/.hms-mirror/aux_libs` directory. This ensures it is loaded in the classpath correctly to support the kerberos connection.  If you're NOT using Kerberos connection, the libraries should be reference in the `jarFile` parameter and NOT be in the `aux_libs` directory.

If you are experimenting with different connection types, ensure the jar file is REMOVED from `aux_libs` when trying other configurations.

- Apache JDBC Driver (packaged with CDP)
> `tbd`
> NOTE: Our experience with this driver is that requires the use of `--hadoop-classpath` in the commandline to load the needed kerberos libraries.


- Cloudera JDBC Driver
> `jdbc:hive2://s04.streever.local:10001;transportMode=http;AuthMech=1;KrbRealm=STREEVER.LOCAL;KrbHostFQDN=s04.streever.local;KrbServiceName=hive;KrbAuthType=2;httpPath=cliservice;SSL=1;AllowSelfSignedCerts=1`
> NOTE: When using this driver, our experience has been that you do NOT need to use `--hadoop-classpath` as a commandline element with versions 1.6.1.0+

## **HDP2 HS2 with No Auth**

Since CDP is usually kerberized AND `hms-mirror` doesn't support the simultanous connections to 2 different kerberos environments, I've setup an HS2 on HDP2 specifically for this effort. NOTE: You need to specify a `username` when connecting to let Hive know what the user is.  No password required.

- Apache Hive Standalone Driver shipped with HDP2.
> `jdbc:hive2://k02.streever.local:10000`


## Direct Metastore DB Acceess for `-epl`

The `LEFT` and `RIGHT` configurations also suppport 'direct' metastore access to collect detailed partition information when using the flag `-epl`.  The support this feature, get the JDBC driver that is appropriate for your metastore(s) backend dbs and place it in `$HOME/.hms-mirror/aux_libs` directory.


