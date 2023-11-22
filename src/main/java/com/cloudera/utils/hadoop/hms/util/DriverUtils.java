/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hadoop.hms.util;

import com.cloudera.utils.hadoop.hms.mirror.Environment;
import com.cloudera.utils.hadoop.hms.mirror.ReportingConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DriverUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DriverUtils.class);

    // This is a shim process that allows us to load a Hive Driver from
    // a jar File, via a new ClassLoader.
    public static Driver getDriver(String driverClassName, String jarFile, Environment environment) {
        Driver hiveShim = null;
        try {
            if (jarFile != null) {
                String[] files = jarFile.split(":");
                URL[] urls = new URL[files.length];
                File[] jarFiles = new File[files.length];
                for (int i=0;i<files.length;i++) {
                    jarFiles[i] = new File(files[i]);
                    if (! jarFiles[i].exists()) {
                        throw new RuntimeException("Jarfile: " + files[i] + " can't be located.");
                    }
                    urls[i] = jarFiles[i].toURI().toURL();
                }

                LOG.trace("Building Classloader to isolate JDBC Library for: " + jarFile);
                URLClassLoader hive3ClassLoader = URLClassLoader.newInstance(urls, jarFiles[0].getClass().getClassLoader());
                LOG.trace("Loading Hive JDBC Driver");
                Class<?> classToLoad = hive3ClassLoader.loadClass(driverClassName);
                Package aPackage = classToLoad.getPackage();
                String implementationVersion = aPackage.getImplementationVersion();
                LOG.info(environment + " - Hive JDBC Implementation Version: " + implementationVersion);
                Driver hiveDriver = (Driver) classToLoad.newInstance();
                LOG.trace("Building Hive Driver Shim");
                hiveShim = new DriverShim(hiveDriver);
                LOG.trace("Registering Hive Shim Driver with JDBC 'DriverManager'");
            } else {
                Class hiveDriverClass = Class.forName(driverClassName);
                hiveShim = (Driver) hiveDriverClass.newInstance();
                Package aPackage = hiveDriverClass.getPackage();
                String implementationVersion = aPackage.getImplementationVersion();
                LOG.info(environment + " - Hive JDBC Implementation Version: " + implementationVersion);
            }
            DriverManager.registerDriver(hiveShim);
        } catch (SQLException | MalformedURLException |
                ClassNotFoundException | InstantiationException |
                IllegalAccessException throwables) {
            throwables.printStackTrace();
            LOG.error(throwables.getMessage(), throwables);
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
