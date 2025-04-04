/*
 * Copyright (c) 2024. David W. Streever All Rights Reserved
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
 */

package com.cloudera.utils.tools;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

@Slf4j
public class ClasspathToZip {

    public ClasspathToZip() {
    }

    public static void main(String[] args) {
        String value = System.getProperty("java.class.path");
        System.out.println("Classpath: " + value);
        ClasspathToZip cp2zip = new ClasspathToZip();
        cp2zip.createZipFileFromClasspath(args[0]);
    }

    public void createZipFileFromClasspath(String zipFileName) {
        List<String> files = new ArrayList<>();
        for (String jarFile : System.getProperty("java.class.path").split(":")) {
            if (jarFile.endsWith("jar") && !jarFile.contains("hms-mirror")) {
                Path absolutePath = Paths.get(jarFile).toAbsolutePath();
                files.add(absolutePath.normalize().toString());
            }
        }
        try {
            createZipFromListofFiles(zipFileName, files);
        } catch (IOException e) {
            log.error("IO issue", e);
        }
    }

    public void createZipFromListofFiles(String zipFileName, List<String> files) throws IOException {

        final FileOutputStream fos = new FileOutputStream(zipFileName);
        ZipOutputStream zipOut = new ZipOutputStream(fos);

        for (String srcFile : files) {
            System.out.println("Adding file: " + srcFile);
            File fileToZip = new File(srcFile);
            if (fileToZip.exists()) {
                FileInputStream fis = null;
                try {
                    fis = new FileInputStream(fileToZip);
                    ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
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

    private List<String> getResourceFiles(String path) throws IOException {
        List<String> filenames = new ArrayList<>();

        // Filter the files in the path and add them to the list.
        Files.list(Paths.get(path)).filter(f -> f.endsWith("jar")).forEach(p -> filenames.add(p.toString()));

        return filenames;
    }
}
