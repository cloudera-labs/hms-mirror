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

package com.cloudera.utils.hms.mirror.domain.support;

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.domain.Messages;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Getter
@Setter
@Slf4j
public class RunStatus implements Comparable<RunStatus>, Cloneable {
    private Date start = null;
    private Date end = null;

    @Schema(description = "This identifies the sessionId that is running/ran for this status.")
    private String sessionId;
    @JsonIgnore
    private Messages errors = null;//new Messages(150);
    @JsonIgnore
    private Messages warnings = null;//new Messages(150);
    //    @JsonIgnore
    Integer concurrency;
    @JsonIgnore
    Future<Boolean> runningTask = null;

    List<String> errorMessages = new ArrayList<>();
    List<String> warningMessages = new ArrayList<>();
    Set<String> configMessages = new TreeSet<>();

    /*
    Track the current progress across the various stages of the operation.
    */
    private Map<StageEnum, CollectionEnum> stages = new LinkedHashMap<>();

    private List<TableMirror> inProgressTables = new ArrayList<>();

    /*
    Maintain statistics on the operation.
     */
    private OperationStatistics operationStatistics = new OperationStatistics();

    private String reportName;
    private String appVersion;
    private ProgressEnum progress = ProgressEnum.INITIALIZED;

    public void clearErrors() {
        if (nonNull(errors)) {
            errors.clear();
        }
    }

    public void clearWarnings() {
        if (nonNull(warnings)) {
            warnings.clear();
        }
    }

    public void addConfigMessage(String message) {
        configMessages.add(message);
    }

    @JsonIgnore
    public long getRuntimeMS() {
        if (nonNull(start) && nonNull(end)) {
            return end.getTime() - start.getTime();
        } else {
            return 0;
        }
    }

    @JsonIgnore
    public long getReturnCode() {
        if (errors == null) {
            return 0;
        } else {
            return errors.getReturnCode();
        }
    }

    @JsonIgnore
    public long getWarningCode() {
        if (warnings == null) {
            return 0;
        } else {
            return warnings.getReturnCode();
        }
    }

    /*
    Flag to indicate if the configuration has been validated.
    This should be reset before each run to ensure the configuration is validated.
     */
//    private boolean configValidated = false;
    /*
    Keep track of the current running state.
     */
//    private ProgressEnum progress = ProgressEnum.INITIALIZED;

    @JsonIgnore
    public long getDuration() {
        long rtn = 0;
        if (nonNull(start) && nonNull(end)) {
            rtn = end.getTime() - start.getTime();
        }
        return rtn;
    }

    @Override
    public int compareTo(RunStatus o) {
        return 0;
    }

//    public ProgressEnum getDynamicProgress() {
//        // If the task is still running, then the progress is still in progress.
//        progress = ProgressEnum.INITIALIZED;
//        if (nonNull(runningTask)) {
//            if (runningTask.isCancelled()) {
//                progress = ProgressEnum.CANCELLED;
//            } else if (!runningTask.isDone()) {
//                progress = ProgressEnum.IN_PROGRESS;
//            } else if (runningTask.isDone()) {
//                // Don't attempt to get the result until the task is done.
//                //     or else it will block.
//                try {
//                    if (runningTask.get()) {
//                        progress = ProgressEnum.COMPLETED;
//                    } else {
//                        progress = ProgressEnum.FAILED;
//                    }
//                } catch (InterruptedException | ExecutionException e) {
//                    //throw new RuntimeException(e);
//                }
//            }
////        } else {
////            progress = ProgressEnum.INITIALIZED;
//        }
//        return progress;
//    }

    public RunStatus() {
        for (StageEnum stage : StageEnum.values()) {
            stages.put(stage, CollectionEnum.WAITING);
        }
    }

    /*
    reset the state of the RunStatus.
     */
    public boolean reset() {
        boolean rtn = Boolean.TRUE;
        if (cancel()) {
            if (nonNull(errors)) {
                errors.clear();
            }
            if (nonNull(warnings)) {
                warnings.clear();
            }
            errorMessages.clear();
            warningMessages.clear();
            // Loop through the stages map and reset the values to WAITING.
            stages.keySet().forEach(k -> stages.put(k, CollectionEnum.WAITING));
            operationStatistics.reset();
            reportName = null;
            start = null;
            end = null;
            if (nonNull(inProgressTables))
                inProgressTables.clear();
        } else {
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    public boolean cancel() {
        boolean rtn = Boolean.TRUE;
        if (nonNull(runningTask) && !runningTask.isDone()) {
            if (runningTask.cancel(true)) {
                log.info("Task cancelled.");
//                this.progress = ProgressEnum.CANCELLED;
            } else {
                log.error("Task could not be cancelled.");
//                this.progress = ProgressEnum.CANCEL_FAILED;
                rtn = Boolean.FALSE;
            }
        }
        return rtn;
    }

    public void setStage(StageEnum stage, CollectionEnum collection) {
        log.info("Setting stage: {} to {}", stage, collection);
        stages.put(stage, collection);
    }

    public CollectionEnum getStage(StageEnum stage) {
        return stages.get(stage);
    }

    public void addError(MessageCode code) {
        if (getErrors() == null) {
            errors = new Messages();
        }
        errors.set(code);
    }

    public void addError(MessageCode code, Object... messages) {
        if (getErrors() == null) {
            errors = new Messages();
        }
        errors.set(code, messages);
    }

    public void addWarning(MessageCode code) {
        if (getWarnings() == null) {
            warnings = new Messages();
        }
        warnings.set(code);
    }

    public void addWarning(MessageCode code, Object... message) {
        if (getWarnings() == null) {
            warnings = new Messages();
        }
        warnings.set(code, message);
    }

    public String getErrorMessage(MessageCode code) {
        return errors.getMessage(code.ordinal());
    }

    public String getWarningMessage(MessageCode code) {
        return warnings.getMessage(code.ordinal());
    }

    public boolean hasConfigMessages() {
        return !configMessages.isEmpty();
    }

    public Boolean hasErrors() {
        if (nonNull(errors) && errors.getReturnCode() != 0) {
            return Boolean.TRUE;
        } else {
            return errorMessages.isEmpty() ? Boolean.FALSE : Boolean.TRUE;
        }
    }

    public Boolean hasWarnings() {
        if (nonNull(warnings) && warnings.getReturnCode() != 0) {
            return Boolean.TRUE;
        } else {
            return warningMessages.isEmpty() ? Boolean.FALSE : Boolean.TRUE;
        }
    }

    public List<String> getErrorMessages() {
        if (errors != null) {
            return errors.getMessages();
        }
        return errorMessages;
    }

    public List<String> getWarningMessages() {
        if (warnings != null) {
            return warnings.getMessages();
        }
        return warningMessages;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (isNull(o) || getClass() != o.getClass()) return false;

        RunStatus runStatus = (RunStatus) o;
        return Objects.equals(start, runStatus.start);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(start);
    }

    public static RunStatus loadConfig(String configFilename) throws IOException {
        RunStatus status;
        // Check if absolute path.
        if (!configFilename.startsWith("/")) {
            // If filename contain a file.separator, assume the location is
            // relative to the working directory.
            if ((configFilename.contains(File.separator))) {
                // Load relative to the working directory.
                File workingDir = new File(".");
                configFilename = workingDir.getAbsolutePath() + File.separator + configFilename;
            } else {
                // Assume it's in the default config directory.
                configFilename = System.getProperty("user.home") + File.separator + ".hms-mirror/cfg"
                        + File.separator + configFilename;
            }
        }
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        URL cfgUrl = null;
        // Load file from classpath and convert to string
        File cfgFile = new File(configFilename);
        if (!cfgFile.exists()) {
            // Try loading from resource (classpath).  Mostly for testing.
            cfgUrl = mapper.getClass().getResource(configFilename);
            if (isNull(cfgUrl)) {
                throw new IOException("Couldn't locate configuration file: " + configFilename);
            }
        } else {
            try {
                cfgUrl = cfgFile.toURI().toURL();
            } catch (MalformedURLException mfu) {
                throw new IOException("Couldn't locate configuration file: "
                        + configFilename, mfu);
            }
        }

        String yamlCfgFile = IOUtils.toString(cfgUrl, StandardCharsets.UTF_8);
        status = mapper.readerFor(RunStatus.class).readValue(yamlCfgFile);

        return status;

    }

    @Override
    public RunStatus clone() throws CloneNotSupportedException {
        RunStatus clone = (RunStatus) super.clone();
        if (nonNull(errors)) {
            clone.errors = errors.clone();
        }
        if (nonNull(warnings)) {
            clone.warnings = warnings.clone();
        }
        clone.errorMessages = new ArrayList<>(errorMessages);
        clone.warningMessages = new ArrayList<>(warningMessages);
        clone.configMessages = new TreeSet<>(configMessages);
        clone.stages = new LinkedHashMap<>(stages);
        clone.inProgressTables = new ArrayList<>(inProgressTables);
        clone.operationStatistics = operationStatistics.clone();
        return clone;
    }
}
