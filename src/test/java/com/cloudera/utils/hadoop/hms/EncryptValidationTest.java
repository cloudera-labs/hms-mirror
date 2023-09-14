/*
 * Copyright (c) 2022-2023. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.Config;
import com.cloudera.utils.hadoop.hms.mirror.MessageCode;
import org.apache.commons.cli.CommandLine;
import org.junit.Test;

import static com.cloudera.utils.hadoop.hms.EnvironmentConstants.ENCRYPTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class EncryptValidationTest extends EndToEndBase {

    private static final String PKEY = "test";

    @Test
    public void decrypt_fail_test() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-pkey", PKEY,
                "-dp", "badencryptedpassword",
                "-cfg", ENCRYPTED
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        CommandLine cmd = mirror.getCommandLine(args);
        Config config = mirror.loadConfig(cmd);
        if (config.hasErrors()) {
            long check = MessageCode.DECRYPTING_PASSWORD_ISSUE.getLong();
            check = check | MessageCode.PASSWORD_CFG.getLong();

            assertEquals(config.getErrors().getReturnCode(),
                    check
            );
        } else {
            assertFalse("Should have failed", Boolean.TRUE);
        }
    }

    @Test
    public void decrypt_test() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-pkey", PKEY,
                "-dp", "FNLmFEI0F/n8acz45c3jVExMounSBklX",
                "-cfg", ENCRYPTED
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        CommandLine cmd = mirror.getCommandLine(args);
        Config config = mirror.loadConfig(cmd);

        String dpassword = config.getWarnings().getMessage(MessageCode.DECRYPTED_PASSWORD.getCode());

        assertEquals("myspecialpassword", dpassword);
    }

    @Test
    public void encrypt_test() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-pkey", PKEY,
                "-p", "myspecialpassword",
                "-cfg", ENCRYPTED
        };
//        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        CommandLine cmd = mirror.getCommandLine(args);
        Config config = mirror.loadConfig(cmd);

        String epassword = config.getWarnings().getMessage(MessageCode.ENCRYPTED_PASSWORD.getCode());

        assertEquals("FNLmFEI0F/n8acz45c3jVExMounSBklX", epassword);

    }

}