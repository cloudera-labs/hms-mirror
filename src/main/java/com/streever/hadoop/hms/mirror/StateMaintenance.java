package com.streever.hadoop.hms.mirror;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.hadoop.hms.Mirror;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class StateMaintenance implements Runnable {
    private static Logger LOG = LogManager.getLogger(StateMaintenance.class);

    private Thread worker;
    private Conversion conversion;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private int sleepInterval;
    private ObjectMapper mapper;
    private String configFile;

    public StateMaintenance(int sleepInterval,
                            final String configFile) {
        this.sleepInterval = sleepInterval;
        this.configFile = configFile;
        mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
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
