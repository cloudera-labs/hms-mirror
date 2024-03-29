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

package com.cloudera.utils.hadoop.hms.mirror;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class StateMaintenance implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StateMaintenance.class);
    private final String startDateStr = null;
    private Thread worker;
    private Conversion conversion;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final int sleepInterval;
    private final ObjectMapper mapper;
    private final String configFile;
    private final String dateMarker;

    public StateMaintenance(int sleepInterval,
                            final String configFile, final String dateMarker) {
        this.sleepInterval = sleepInterval;
        this.configFile = configFile;
        mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // TODO: Need to use this to record a time specific retry file because the current one gets overwritten when
        //       the process is run again and we lose some start to debug from.
        this.dateMarker = dateMarker;
    }

    public Conversion getConversion() {
        return conversion;
    }

    public void setConversion(Conversion conversion) {
        this.conversion = conversion;
    }

    public void start() {
        assert (conversion != null);
        worker = new Thread(this);
        worker.start();
    }

    public void stop() {
        running.set(false);
    }

    @Override
    public void run() {
        if (conversion != null) {
            running.set(true);
            while (running.get()) {
                saveState();
                try {
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("StateMaintenance thread interrupted");
                }
            }
        } else {
            throw new RuntimeException("'conversion' hasn't been set");
        }
    }

    // Retry file name is based on the config file.
    public File getRetryFile() {
        String wrkRetryFile = configFile.substring(configFile.lastIndexOf(System.getProperty("file.separator")) + 1);
        wrkRetryFile = wrkRetryFile.substring(0, wrkRetryFile.lastIndexOf("."));
        // Materialize it.
        wrkRetryFile = System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror" +
                System.getProperty("file.separator") + "retry" + System.getProperty("file.separator") + wrkRetryFile + ".retry";

        File retryFile = new File(wrkRetryFile);
        return retryFile;
    }

    public File getRetryMarkerFile() {
        String wrkRetryFile = configFile.substring(configFile.lastIndexOf(System.getProperty("file.separator")) + 1);
        wrkRetryFile = wrkRetryFile.substring(0, wrkRetryFile.lastIndexOf("."));
        // Materialize it.
        wrkRetryFile = System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror" +
                System.getProperty("file.separator") + "retry" + System.getProperty("file.separator") + wrkRetryFile +
                "_" + dateMarker + ".retry";

        File retryFile = new File(wrkRetryFile);
        return retryFile;
    }

    public void saveState() {
        // Write out to the ~/.hms-mirror/retry directory.
        if (conversion != null) {
            try {
                String conversionStr = mapper.writeValueAsString(conversion);
                File retryFile = null;
                FileWriter retryFileWriter = null;
                try {
                    retryFile = getRetryFile();
                    retryFileWriter = new FileWriter(retryFile);
                    retryFileWriter.write(conversionStr);
                    LOG.debug("Retry State 'saved' to: " + retryFile.getPath());
                } catch (IOException ioe) {
                    LOG.error("Problem 'writing' Retry File", ioe);
                } finally {
                    retryFileWriter.close();
                }
                try {
                    retryFile = getRetryMarkerFile();
                    retryFileWriter = new FileWriter(retryFile);
                    retryFileWriter.write(conversionStr);
                    LOG.debug("Retry State 'saved' to: " + retryFile.getPath());
                } catch (IOException ioe) {
                    LOG.error("Problem 'writing' Retry File", ioe);
                } finally {
                    retryFileWriter.close();
                }
            } catch (JsonProcessingException e) {
                LOG.error("Problem 'saving' Retry state", e);
            } catch (IOException ioe) {
                LOG.error("Problem 'closing' Retry File", ioe);
            }
        } else {
            LOG.error("'conversion' hasn't been set'");
        }
    }

    public void deleteState() {
        // Write out to the ~/.hms-mirror/retry directory.
        File retryFile = null;
        retryFile = getRetryFile();
        retryFile.delete();
        LOG.debug("Retry State 'deleted' to: " + retryFile.getPath());
    }
}
