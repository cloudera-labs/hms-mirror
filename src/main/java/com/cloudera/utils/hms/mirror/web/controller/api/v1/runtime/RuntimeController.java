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

package com.cloudera.utils.hms.mirror.web.controller.api.v1.runtime;

import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.ReportService;
import com.cloudera.utils.hms.mirror.web.service.RuntimeService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Set;

@CrossOrigin
@RestController
@Slf4j
@RequestMapping(path = "/api/v1/runtime")
public class RuntimeController {

    private ExecuteSessionService executeSessionService;
    private ReportService reportService;
    private RuntimeService runtimeService;

    @Autowired
    public void setHmsMirrorCfgService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setReportService(ReportService reportService) {
        this.reportService = reportService;
    }

    @Autowired
    public void setRuntimeService(RuntimeService runtimeService) {
        this.runtimeService = runtimeService;
    }

    @Operation(summary = "Start the Operation")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation started successfully",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = RunStatus.class))})
//            , @ApiResponse(responseCode = "400", description = "Invalid id supplied",
//                    content = @Content)
//            , @ApiResponse(responseCode = "404", description = "Config not found",
//                    content = @Content)
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/start")
    public RunStatus start(
//            @RequestParam(name = "sessionId", required = false) String sessionId,
                           @RequestParam(name = "dryrun") Boolean dryrun,
//                           @RequestParam(name = "autoGLM", required = false) Boolean autoGLM,
                           @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws MismatchException, RequiredConfigurationException, SessionException, EncryptionException {
//        boolean lclAutoGLM = autoGLM != null && autoGLM;
        return runtimeService.start(dryrun,
//                lclAutoGLM,
                maxThreads);
    }


    @Operation(summary = "Cancel the Operation")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation cancelled successfully",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = RunStatus.class))})
//            , @ApiResponse(responseCode = "400", description = "Invalid id supplied",
//                    content = @Content)
//            , @ApiResponse(responseCode = "404", description = "Config not found",
//                    content = @Content)
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/cancel")
    public RunStatus cancel() {
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();
        runStatus.cancel();
        return runStatus;
    }

    @Operation(summary = "Get Reports for Session")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Reports retrieved successfully",
                    content = {@Content(mediaType = "application/zip",
                            schema = @Schema(implementation = HttpEntity.class))})
//            , @ApiResponse(responseCode = "400", description = "Invalid id supplied",
//                    content = @Content)
//            , @ApiResponse(responseCode = "404", description = "Config not found",
//                    content = @Content)
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/reports/latest/download")
    public HttpEntity<ByteArrayResource> downloadLatestSessionReport() throws IOException {
        Set<String> availableReports = reportService.getAvailableReports();
        if (availableReports.isEmpty()) {
            throw new IOException("No reports available");
        } else {
            return reportService.getZippedReport(availableReports.iterator().next());
        }
    }

    @Operation(summary = "Get Reports for Session")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Reports retrieved successfully",
                    content = {@Content(mediaType = "application/zip",
                            schema = @Schema(implementation = HttpEntity.class))})
//            , @ApiResponse(responseCode = "400", description = "Invalid id supplied",
//                    content = @Content)
//            , @ApiResponse(responseCode = "404", description = "Config not found",
//                    content = @Content)
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/reports/{id}/download")
    public HttpEntity<ByteArrayResource> downloadSessionReport(@PathVariable @NotNull String id) throws IOException {
        return reportService.getZippedReport(id);
    }


    @Operation(summary = "Available Reports")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "List of available reports",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Set.class))})
//            , @ApiResponse(responseCode = "400", description = "Invalid id supplied",
//                    content = @Content)
//            , @ApiResponse(responseCode = "404", description = "Config not found",
//                    content = @Content)
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/reports/list")
    public Set<String> availableReports() {
        return reportService.getAvailableReports();
    }

}
