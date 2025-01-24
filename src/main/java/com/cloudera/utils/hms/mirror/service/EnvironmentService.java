/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Slf4j
public class EnvironmentService {

    public void setupGSS() {
        try {
            String CURRENT_USER_PROP = "current.user";

            String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
            String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};

            // Get a value that over rides the default, if nothing then use default.
            String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");

            // Set a default
            if (isBlank(hadoopConfDirProp))
                hadoopConfDirProp = "/etc/hadoop/conf";

            Configuration hadoopConfig = new Configuration(true);

            File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
            for (String file : HADOOP_CONF_FILES) {
                File f = new File(hadoopConfDir, file);
                if (f.exists()) {
                    log.debug("Adding conf resource: '{}'", f.getAbsolutePath());
                    try {
                        // I found this new Path call failed on the Squadron Clusters.
                        // Not sure why.  Anyhow, the above seems to work the same.
                        hadoopConfig.addResource(new Path(f.getAbsolutePath()));
                    } catch (Throwable t) {
                        // This worked for the Squadron Cluster.
                        // I think it has something to do with the Docker images.
                        hadoopConfig.addResource("file:" + f.getAbsolutePath());
                    }
                }
            }

            // hadoop.security.authentication
            if (hadoopConfig.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
                try {
                    UserGroupInformation.reset();
                    UserGroupInformation.setConfiguration(hadoopConfig);
                    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                    String user = ugi.getShortUserName();
                    String auth = ugi.getAuthenticationMethod().toString();
                    log.info("User: {} with auth: {}", user, auth);
                    // We should be logged in via Kerberos.
                    if (!auth.equalsIgnoreCase("KERBEROS")) {
                        throw new RuntimeException("Kerberos auth required as seen in configs.  (" + user + ":" + auth + ").");
                    }
                } catch (Throwable t) {
                    // Revert to non JNI. This happens in Squadron (Docker Imaged Hosts)
                    UserGroupInformation.reset();
                    log.error("Failed GSS Init.  Attempting different Group Mapping");
                    hadoopConfig.set("hadoop.security.group.mapping", "org.apache.hadoop.security.ShellBasedUnixGroupsMapping");
                    UserGroupInformation.setConfiguration(hadoopConfig);
                    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                    String user = ugi.getShortUserName();
                    String auth = ugi.getAuthenticationMethod().toString();
                    log.info("User: {} with auth: {}", user, auth);
                    // We should be logged in via Kerberos.
                    if (!auth.equalsIgnoreCase("KERBEROS")) {
                        throw new RuntimeException("Kerberos auth required as seen in configs.  (" + user + ":" + auth + ").");
                    }
                }
            }
        } catch (IOException ioe) {
            log.error("Issue initializing Configurations", ioe);
            throw new RuntimeException(ioe);
        }
    }

}
