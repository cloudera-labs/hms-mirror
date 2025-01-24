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

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.jcabi.manifests.Manifests;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.text.MessageFormat;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Getter
@Setter
@Slf4j
public class ExecuteSession implements Cloneable {

    private String sessionId;
//    private String passwordKey;

    //    private final AtomicBoolean running = new AtomicBoolean(false);
    private boolean connected = Boolean.FALSE;
    private Connections connections = new Connections();
    private RunStatus runStatus;
    //    private HmsMirrorConfig resolvedConfig;
    private HmsMirrorConfig config;
//    private CliEnvironment cliEnvironment;
    private Conversion conversion;
    
    public void addError(MessageCode code) {
        getRunStatus().addError(code);
    }

    public void addError(MessageCode code, Object... args) {
        getRunStatus().addError(code, args);
    }

    public void addWarning(MessageCode code) {
        getRunStatus().addWarning(code);
    }

    public void addWarning(MessageCode code, Object... args) {
        getRunStatus().addWarning(code, args);
    }

    public void close() throws SessionException {
        if (this.isRunning()) {
            throw new SessionException("Session is still running.  You can't change the session while it is running.");
        }

    }
    /*
    Placeholder to save messages on background adjustments to a configuration done during validation.
     */
    public void addConfigAdjustmentMessage(DataStrategyEnum strategy, String property, String from, String to, String why) {
        String message = MessageFormat.format("Adjusted Data Strategy: {0}  for property {1} from {2} to {3}. Reason: {4}",
                strategy.toString(), property, from, to, why);
                getRunStatus().addConfigMessage(message);
    }

    public boolean isRunning() {
        boolean running = Boolean.FALSE;
        if (nonNull(runStatus)) {
            switch (runStatus.getProgress()) {
                case STARTED:
                case IN_PROGRESS:
                    running = Boolean.TRUE;
                    break;
                case INITIALIZED:
                case COMPLETED:
                case FAILED:
                case CANCELLED:
                case CANCEL_FAILED:
                default:
                    running = Boolean.FALSE;
                    break;
            }
        } else {
            running = Boolean.FALSE;
        }
        return running;
    }

//    public void clearRunStatus() {
//        if (nonNull(runStatus)) {
//            runStatus = null;
//        }
//    }
    public RunStatus getRunStatus() {
        if (isNull(runStatus)) {
            this.runStatus = new RunStatus();
            try {
                this.runStatus.setAppVersion(Manifests.read("HMS-Mirror-Version"));
            } catch (IllegalArgumentException iae) {
                this.runStatus.setAppVersion("Unknown");
            }
        }
        return runStatus;
    }

    public void resetConnectionStatuses() {
        this.connections.reset();
        runStatus.setProgress(ProgressEnum.INITIALIZED);
        connected = Boolean.FALSE;
    }

    @Override
    public ExecuteSession clone() {
        try {
            ExecuteSession clone = (ExecuteSession) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            if (nonNull(config)) {
                clone.config = config.clone();
                // resolvedConfig is a derived object, so we don't need to clone it.
            }
            if (nonNull(connections)) {
                clone.connections = connections.clone();
            }
//            if (nonNull(conversion)) {
//                clone.conversion = conversion.clone();
//            }
            if (nonNull(runStatus)) {
                clone.runStatus = runStatus.clone();
            }
//            clone.cliEnvironment = cliEnvironment; // This isn't a cloneable object. Just establish the reference.
            // Don't clone the other parts: runStatus, cliEnvironment, conversion, runResults
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

}
