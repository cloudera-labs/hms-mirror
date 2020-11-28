package com.streever.hadoop.hms.util;

import com.streever.hadoop.hms.mirror.Cluster;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DriverUtils {
    private static Logger LOG = LogManager.getLogger(DriverUtils.class);

    // This is a shim process that allows us to load a Hive Driver from
    // a jar File, via a new ClassLoader.
    public static Driver getDriver(String jarFile) {
        Driver hiveShim = null;
        try {
            File jdbcJar = new File(jarFile);
            URL[] urls = {jdbcJar.toURI().toURL()};
            LOG.trace("Building Classloader to isolate JDBC Library for: " + jarFile);
            URLClassLoader hive3ClassLoader = URLClassLoader.newInstance(urls, jdbcJar.getClass().getClassLoader());
            LOG.trace("Loading Hive JDBC Driver");
            Class<?> classToLoad = hive3ClassLoader.loadClass("org.apache.hive.jdbc.HiveDriver");
            Driver hiveDriver = (Driver) classToLoad.newInstance();
            LOG.trace("Building Hive Driver Shim");
            hiveShim = new DriverShim(hiveDriver);
            LOG.trace("Registering Hive Shim Driver with JDBC 'DriverManager'");
            DriverManager.registerDriver(hiveShim);
        } catch (SQLException | MalformedURLException |
                ClassNotFoundException | InstantiationException |
                IllegalAccessException throwables) {
            throwables.printStackTrace();
        }
        return hiveShim;
    }

    public static void deregisterDriver(Driver hiveShim) {
        try {
            LOG.trace("De-registering Driver from 'DriverManager'");
            DriverManager.deregisterDriver(hiveShim);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
