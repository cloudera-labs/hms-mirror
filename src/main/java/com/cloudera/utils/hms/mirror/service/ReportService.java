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

import com.cloudera.utils.hms.mirror.domain.DBMirror;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.cloudera.utils.hms.util.UrlUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import static com.cloudera.utils.hms.mirror.web.controller.ControllerReferences.*;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * Service for managing reports, including creating, retrieving, archiving, and
 * packaging reports into a zip format. This class interacts with underlying
 * domain services and file systems to provide functionalities for report handling.
 * It primarily manages session-based reports and stored artifacts.
 */
@Component
@Slf4j
@Getter
@Setter
public class ReportService {

    private final DomainService domainService;
    private final ExecuteSessionService executeSessionService;

    public ReportService(DomainService domainService, ExecuteSessionService executeSessionService) {
        this.domainService = domainService;
        this.executeSessionService = executeSessionService;
    }

    /**
     * Creates a ZIP file from all files in the specified directory.
     *
     * @param zipFileName   the name of the ZIP file to be created
     * @param baseDirectory the path to the directory whose files will be added to the ZIP
     * @throws IOException if an I/O error occurs while creating or writing the ZIP file
     */
    private void createZipFromDirectory(String zipFileName, String baseDirectory) throws IOException {
        final FileOutputStream fos = new FileOutputStream(zipFileName);
        ZipOutputStream zipOut = new ZipOutputStream(fos);
        File sessionDirectory = new File(baseDirectory);
        String[] files = sessionDirectory.list();
        for (String srcFile : files) {
            System.out.println("Adding file: " + srcFile);
            String absolutePath = baseDirectory + File.separator + srcFile;
            File fileToZip = new File(absolutePath);
            if (fileToZip.exists()) {
                FileInputStream fis = null;
                try {
                    fis = new FileInputStream(fileToZip);
                    ZipEntry zipEntry = new ZipEntry(srcFile);
                    zipOut.putNextEntry(zipEntry);
                    byte[] bytes = new byte[1024];
                    int length;
                    while ((length = fis.read(bytes)) >= 0) {
                        zipOut.write(bytes, 0, length);
                    }
                } catch (ZipException ze) {
                    log.error(ze.getMessage(), ze);
                } finally {
                    assert fis != null;
                    fis.close();
                }
            } else {
                System.out.println("File not found: " + srcFile);
            }
        }
        zipOut.close();
        fos.close();
    }

    /**
     * Archives the report by moving it from the current report directory to an archive directory.
     * The directory paths are constructed based on the provided report ID.
     *
     * @param report_id the unique identifier of the report to be archived
     */
    public void archiveReport(String report_id) {
        // Using the 'id', get the reports for the session.
        String outputDirectory = executeSessionService.getReportOutputDirectory();
        String reportDirectoryName = outputDirectory + File.separator + report_id;
        String baseDir = UrlUtils.removeLastDirFromUrl(outputDirectory);
        String archiveDirectoryName = baseDir + File.separator + "archive";

        File archiveDirectory = new File(archiveDirectoryName);
        if (!archiveDirectory.exists()) {
            archiveDirectory.mkdirs();
        }

        // Handle nested directories in REPORT_ID
        String archiveReportDirectoryName = archiveDirectoryName + File.separator + report_id;
        File archiveReportDirectoryParent = new File(archiveReportDirectoryName);
        if (!archiveReportDirectoryParent.exists()) {
            archiveReportDirectoryParent.mkdirs();
        }

        try {
            Files.move(new File(reportDirectoryName).toPath(), new File(archiveReportDirectoryName).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.error("Error moving report to archive: {}", e.getMessage(), e);
        }
    }

    /**
     * Retrieves and categorizes the artifacts from a specified report session directory.
     * The artifacts are grouped into categories such as databases, distcp plans,
     * and other files based on predefined naming conventions.
     *
     * @param id the unique identifier of the report session, used to locate its directory
     * @return a map where the keys represent artifact categories
     *         and the values are lists of artifact names within those categories
     */
    public Map<String, List<String>> reportArtifacts(String id) {
        Map<String, List<String>> artifacts = new TreeMap<>();

        // Using the 'id', get the reports for the session.
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        // List directories in the report directory.
        String sessionDirectoryName = reportDirectory + File.separator + id;
        File sessionDirectory = new File(sessionDirectoryName);

        String[] files = sessionDirectory.list();

        for (String srcFile : files) {
            if (srcFile.endsWith("_hms-mirror.yaml")) {
                String databaseName = srcFile.substring(0, srcFile.indexOf("_hms-mirror.yaml"));

                List<String> databases = artifacts.get(DATABASES);//.add(databaseName);
                if (isNull(databases)) {
                    databases = new ArrayList<>();
                    artifacts.put(DATABASES, databases);
                }
                databases.add(databaseName);
            } else if (srcFile.endsWith("distcp_plans.yaml")) {
                List<String> distcp_plans = artifacts.get(DISTCP_PLANS);//.add(databaseName);
                if (isNull(distcp_plans)) {
                    distcp_plans = new ArrayList<>();
                    artifacts.put(DISTCP_PLANS, distcp_plans);
                }
                distcp_plans.add(srcFile);
            } else {
                List<String> others = artifacts.get(OTHERS);
                if (isNull(others)) {
                    others = new ArrayList<>();
                    artifacts.put(OTHERS, others);
                }
                others.add(srcFile);
            }
        }
        return artifacts;
    }

    public String getDatabaseFile(String sessionId, String database) {
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        return reportDirectory + File.separator + sessionId + File.separator + database + "_hms-mirror.yaml";
    }

    public String getReportFile(String sessionId, String reportFile) {
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        return reportDirectory + File.separator + sessionId + File.separator + reportFile;
    }

    public String getSessionConfigFile(String sessionId) {
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        return reportDirectory + File.separator + sessionId + File.separator + "session-config.yaml";
    }

    public String getSessionRunStatusFile(String sessionId) {
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        return reportDirectory + File.separator + sessionId + File.separator + "run-status.yaml";
    }

    public String getDistcpWorkbookFile(String sessionId, String file) {
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        return reportDirectory + File.separator + sessionId + File.separator + file;
    }

    public HmsMirrorConfig getConfig(String sessionId) {
        String configFile = getSessionConfigFile(sessionId);
        log.info("Loading Config File: {}", configFile);
        HmsMirrorConfig config = domainService.deserializeConfig(configFile);
        return config;
    }

    /**
     * Retrieves a distributed copy (DistCp) workbook for a given session and file.
     * The method loads and deserializes the workbook from the specified session directory.
     *
     * @param sessionId the unique identifier for the session
     * @param file the name of the workbook file to be processed
     * @return a map representing the deserialized DistCp workbook, where the keys are string-based
     *         categories, and the values are nested maps consisting of string keys and sets of strings
     */
    public Map<String, Map<String, Set<String>>> getDistCpWorkbook(String sessionId, String file) {
        String distcpWorkbookFile = getDistcpWorkbookFile(sessionId, file);
        log.info("Loading DistCp Workbook File: {}", distcpWorkbookFile);
        Map<String, Map<String, Set<String>>> distcpPlan = domainService.deserializeDistCpWorkbook(distcpWorkbookFile);
        return distcpPlan;
    }

    public RunStatus getRunStatus(String sessionId) throws IOException {
        String runStatusFile = getSessionRunStatusFile(sessionId);
        log.info("Loading RunStatus File: {}", runStatusFile);
        RunStatus status = null;
        try {
            status = RunStatus.loadConfig(runStatusFile);
        } catch (IOException e) {
            log.error("Error loading RunStatus file: {}", runStatusFile, e);
            throw e;
        }
        return status;
    }


    public DBMirror getDBMirror(String sessionId, String database) {
        String databaseFile = getDatabaseFile(sessionId, database);
        DBMirror dbMirror = domainService.deserializeDBMirror(databaseFile);
        log.info("Report loaded.");
        return dbMirror;
    }

    public String getReportFileString(String sessionId, String file) {
        String reportFile = getReportFile(sessionId, file);
        String asString = domainService.fileToString(reportFile);
        return asString;
    }

    /**
     * Generates a ZIP archive containing the reports for a given session ID and returns it as an HTTP entity.
     * The method locates the session directory based on the `id`, compresses its contents into a ZIP file,
     * and prepares it for download with relevant HTTP headers.
     *
     * @param id the unique identifier of the session whose reports are to be zipped
     * @return an HTTP entity containing the ZIP archive of the session reports as a byte array resource
     * @throws IOException if the session directory does not exist, is not a directory, or there is an error during file processing
     */
    public HttpEntity<ByteArrayResource> getZippedReport(String id) throws IOException {
        // Using the 'id', get the reports for the session.
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        // List directories in the report directory.
        String sessionDirectoryName = reportDirectory + File.separator + id;
        File sessionDirectory = new File(sessionDirectoryName);
        // Ensure it exists and is a directory.
        if (!sessionDirectory.exists() || !sessionDirectory.isDirectory()) {
            throw new IOException("Session reports not found.");
        }

        String lastDir = NamespaceUtils.getLastDirectory(id);
        // Zip the files in the report directory and return the zip file.
        String zipFileName = System.getProperty("java.io.tmpdir") + File.separator + lastDir + ".zip";

        createZipFromDirectory(zipFileName, sessionDirectoryName);

        // Package and return the zip file.
        HttpHeaders header = new HttpHeaders();
        header.setContentType(new MediaType("application", "force-download"));

        String downloadFilename = id + ".zip";
        header.set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + downloadFilename);

        return new HttpEntity<>(new ByteArrayResource(Files.readAllBytes(Paths.get(zipFileName))), header);
    }

    /**
     * Retrieves the set of available report directories that contain valid session configuration files.
     * The method scans the report output directory for subdirectories, checks if they contain
     * valid session configuration files (e.g., `session-config.yaml`), and returns the names
     * of these directories in descending order.
     *
     * @return a set of strings representing the names of directories that contain valid session reports,
     *         sorted in descending order.
     */
    public Set<String> getAvailableReports() {
        Set<String> rtn = new TreeSet<>(new Comparator<String>() {
            // Descending order.
            @Override
            public int compare(String o1, String o2) {
                String l1 = o1;
                String l2 = o2;
                // Sort decending order.
                if (nonNull(l1) && nonNull(l2)) {
                    return l2.compareTo(l1);
                } else {
                    return o2.compareTo(o1);
                }
            }
        });
        // Validate that the report id directory exists.
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        // List directories in the report directory.
        File folder = new File(reportDirectory);
        if (folder.isDirectory()) {
            Collection<File> files = FileUtils.listFilesAndDirs(folder, new NotFileFilter(TrueFileFilter.INSTANCE), DirectoryFileFilter.DIRECTORY);
            for (File file : files) {
                String filename = file.getPath();
                if (file.getPath().equals(reportDirectory)) {
                    // Skip putting the report dir in the list.
                    continue;
                }
                // Get listing of files in the directory. Filter on yaml files.
                Collection<File> dirFiles = FileUtils.listFiles(new File(filename), new String[]{"yaml"}, false);
                //  We only want to list it if it has some report files in it.
                boolean found = Boolean.FALSE;
                for (File dirFile : dirFiles) {
                    // Look for the session-config.yaml file.
                    if (dirFile.getName().endsWith("session-config.yaml")) {
                        found = Boolean.TRUE;
                        break;
                    }
                }
                // If we found the session yaml, then we can assume it's an actual report directory and add it to the list.
                if (found) {
                    try {
                        filename = filename.substring(reportDirectory.length() + 1);
                        rtn.add(filename);
                    } catch (StringIndexOutOfBoundsException e) {
                        log.error("Error processing file: {}", filename);
                    }
                }
            }
        } else {
            // Throw exception that output directory isn't a directory.
        }
        return rtn;
    }
}
