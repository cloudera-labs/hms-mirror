package com.cloudera.utils.hms.mirror.domain.support;

import com.cloudera.utils.hms.mirror.domain.HiveServer2Config;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.cloudera.utils.hms.mirror.domain.HiveServer2Config.APACHE_HIVE_DRIVER;
import static com.cloudera.utils.hms.mirror.domain.HiveServer2Config.CLOUDERA_HIVE_DRIVER;

@Slf4j
public enum HiveDriverEnum {

    APACHE_HIVE(APACHE_HIVE_DRIVER, Arrays.asList("host", "port",
            "dbName", "sessionConfs", "hiveConfs", "hiveVars", " transportMode", "principal", "saslQop", "user",
            "password", "ssl", "sslTrustStore", "trustStorePassword","serviceDiscoveryMode","zookeeperKeyStoreType",
            "zookeeperNamespace", "retries")),
    CLOUDERA_HIVE(CLOUDERA_HIVE_DRIVER, Arrays.asList("AllowSelfSignedCerts",
            "AsyncExecPollInterval","AuthMech","BinaryColumnLength","CAIssuedCertsMismatch","CatalogSchemaSwitch",
            "DecimalColumnScale","DefaultStringColumnLength","DelegationToken","DelegationUID",
            "FastConnection","httpPath","JWTString","IgnoreTransactions", "KrbAuthType",
            "KrbHostFQDN","KrbRealm","KrbServiceName","LoginTimeout",
            "LogLevel","LogPath","NonRowcountQueryPrefixes","PreparedMetaLimitZero","PWD",
            "RowsFetchedPerBlock","SocketTimeout","SSL","SSLKeyStore","SSLKeyStoreProvider",
            "SSLKeyStorePwd","SSLKeyStoreType","SSLTrustStore","SSLTrustStoreProvider",
            "SSLTrustStorePwd","SSLTrustStoreType","TransportMode",
            "UID","UseNativeQuery","zk"));


    private final String driverClassName;
    // A list of supported driver parameters
    private final List<String> driverParameters;

    HiveDriverEnum(String driverClassName, List<String> driverParameters) {
        this.driverClassName = driverClassName;
        this.driverParameters = driverParameters;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public List<String> getDriverParameters() {
        return driverParameters;
    }

    public static String[] getDriverClassNames() {
        return Arrays.stream(HiveDriverEnum.values())
                .map(HiveDriverEnum::getDriverClassName)
                .toArray(String[]::new);
    }

    public static HiveDriverEnum getDriverEnum(String driverClassName) {
        return Arrays.stream(HiveDriverEnum.values())
                .filter(driver -> driver.getDriverClassName().equals(driverClassName))
                .findFirst()
                .orElse(null);
    }

    public static List<String> getDriverParameters(String driverClassName) {
        return Arrays.stream(HiveDriverEnum.values())
                .filter(driver -> driver.getDriverClassName().equals(driverClassName))
                .findFirst()
                .map(HiveDriverEnum::getDriverParameters)
                .orElse(null);
    }

    /**
     * Remove any properties that aren't allowed by the driver.  Not allowed is defined as not being one of the
     * properties in this class.
     *
     * @param properties
     * @return
     */
    public Properties reconcileForDriver(Properties properties) {
        Properties reconciledProperties = new Properties();
        for (String key : properties.stringPropertyNames()) {
            if (driverParameters.contains(key)) {
                reconciledProperties.put(key, properties.getProperty(key));
            } else {
                // TODO: Add and Warn OR omit and LOG.
                log.warn("Property " + key + " is not supported by driver " + driverClassName);
            }
        }
        return reconciledProperties;
    }

}