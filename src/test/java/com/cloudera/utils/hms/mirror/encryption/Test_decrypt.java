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

package com.cloudera.utils.hms.mirror.encryption;

import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.password.PasswordApp;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = PasswordApp.class,
        args = {
//                "--hms-mirror.config.data-strategy=EXPORT_IMPORT",
                "--hms-mirror.config.password-key=test",
                "--hms-mirror.config.decrypt-password=FNLmFEI0F/n8acz45c3jVExMounSBklX",
//                "--hms-mirror.config.migrate-acid=true",
//                "--hms-mirror.config.migrate-acid-only=true",
//                "--hms-mirror.config.warehouse-directory=/warehouse/managed",
//                "--hms-mirror.config.external-warehouse-directory=/warehouse/external",
//                "--hms-mirror.config.sort-dynamic-partition-inserts=true",
//                "--hms-mirror.config.downgrade-acid=true",
//                "--hms-mirror.config.read-only=true",
//                "--hms-mirror.config.sync=true",
//                "--hms-mirror.config.evaluate-partition-location=true",
//                "--hms-mirror.config.intermediate-storage=s3a://my_is_bucket",
//                "--hms-mirror.config.target-namespace=s3a://my_cs_bucket",
//                "--hms-mirror.config.reset-to-default-location=true",
//                "--hms-mirror.config.distcp=true",
//                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
//                "--hms-mirror.config.filename=/config/default.yaml.encrypted",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/encryption/decrypt_test"
        })
@Slf4j
public class Test_decrypt extends PasswordTestBase {

    @Test
    public void validateDecryptPassword() throws EncryptionException {

        String decryptedPassword = doIt();

        assertEquals( "myspecialpassword",
                decryptedPassword, "Decrypt PasswordApp Failure: ");
    }

}
