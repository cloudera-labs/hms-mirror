package com.cloudera.utils.hadoop.hms.mirror;

import org.junit.Test;

import java.io.IOException;

public class ConfigLoadTest {

    @Test
    public void configSelfTest_01() {
        try {
            Config config = ConfigTest.deserializeResource("/config/default.self.yaml");
            System.out.println("hello");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
