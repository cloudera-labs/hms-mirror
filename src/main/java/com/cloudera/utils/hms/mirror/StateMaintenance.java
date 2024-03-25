/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Getter
@Setter
@Slf4j
public class StateMaintenance
{

    private final String startDateStr = null;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final int sleepInterval = 1000;
    private Thread worker;
    private Conversion conversion;

    /*
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
                    log.debug("Retry State 'saved' to: " + retryFile.getPath());
                } catch (IOException ioe) {
                    log.error("Problem 'writing' Retry File", ioe);
                } finally {
                    retryFileWriter.close();
                }
                try {
                    retryFile = getRetryMarkerFile();
                    retryFileWriter = new FileWriter(retryFile);
                    retryFileWriter.write(conversionStr);
                    log.debug("Retry State 'saved' to: " + retryFile.getPath());
                } catch (IOException ioe) {
                    log.error("Problem 'writing' Retry File", ioe);
                } finally {
                    retryFileWriter.close();
                }
            } catch (JsonProcessingException e) {
                log.error("Problem 'saving' Retry state", e);
            } catch (IOException ioe) {
                log.error("Problem 'closing' Retry File", ioe);
            }
        } else {
            log.error("'conversion' hasn't been set'");
        }
    }

    public void deleteState() {
        // Write out to the ~/.hms-mirror/retry directory.
        File retryFile = null;
        retryFile = getRetryFile();
        retryFile.delete();
        log.debug("Retry State 'deleted' to: " + retryFile.getPath());
    }
    */

}
