# Example URL&apos;s

## **CDP Hive via Knox Gateway**

Doesn't require Kerberos.  Knox is SSL, so depending on whether you've self-signed your certs you may need to make adjustments.

- Apache Hive and CDP Packaged Apache Hive JDBC Driver

```
jdbc:hive2://s03.streever.local:8443/;ssl=true;transportMode=http;httpPath=gateway/cdp-proxy-api/hive;sslTrustStore=/Users/dstreev/bin/certs/gateway-client-trust.jks;trustStorePassword=changeit
```

- Cloudera JDBC Driver

```
jdbc:hive2://s03.streever.local:8443;transportMode=http;AuthMech=3;httpPath=gateway/cdp-proxy-api/hive;SSL=1;AllowSelfSignedCerts=1
```

## **CDP Hive direct with Kerberos**

When connecting to via Kerberos, configure the jar files the same way as non-kerberized connections.

- Apache Hive and CDP Packaged Apache Hive JDBC Driver

```
jdbc:hive2://s04.streever.local:10001/;ssl=true;transportMode=http;httpPath=cliservice;sslTrustStore=/home/dstreev/bin/certs/gateway-client-trust.jks;trustStorePassword=changeit;principal=hive/_HOST@STREEVER.LOCAL
```

> NOTE: This configuration includes a certificate reference for SSL.  If you're using self-signed certs, you'll need to adjust the `sslTrustStore` and `trustStorePassword` values.

- Cloudera JDBC Driver

```
jdbc:hive2://s04.streever.local:10001;transportMode=http;AuthMech=1;KrbRealm=STREEVER.LOCAL;KrbHostFQDN=s04.streever.local;KrbServiceName=hive;KrbAuthType=2;httpPath=cliservice;SSL=1;AllowSelfSignedCerts=1
```

## **HDP2 HS2 with No Auth**

Since CDP is usually kerberized AND `hms-mirror` doesn't support the simultanous connections to 2 different kerberos environments, I've setup an HS2 on HDP2 specifically for this effort. NOTE: You need to specify a `username` when connecting to let Hive know what the user is.  No password required.

- Apache Hive Standalone Driver shipped with HDP2.

```
jdbc:hive2://k02.streever.local:10000
```


## Direct Metastore DB Access

The `LEFT` and `RIGHT` configurations also suppport 'direct' metastore access to collect detailed partition information.  The support this feature, get the JDBC driver that is appropriate for your metastore(s) backend dbs and place it in `$HOME/.hms-mirror/aux_libs` directory.

    
