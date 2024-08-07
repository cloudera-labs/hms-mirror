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

package com.cloudera.utils.hms.mirror.util;

import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@Slf4j
public class ThreadPoolConfigurator {

    @Bean("jobThreadPool")
    @Order(20)
    @ConditionalOnProperty(
            name = "hms-mirror.concurrency.max-threads")
    public TaskExecutor jobThreadPool(ExecuteSessionService executeSessionService,  @Value("${hms-mirror.concurrency.max-threads}") Integer value) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        log.info("Setting up jobThreadPool with max threads: {}", value);

        executor.setCorePoolSize(value);
        executor.setMaxPoolSize(value);

        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.setThreadNamePrefix("job-");
        executor.initialize();
        return executor;
    }

    @Bean("metadataThreadPool")
    @Order(20)
    @ConditionalOnProperty(
            name = "hms-mirror.concurrency.max-threads")
    public TaskExecutor metadataThreadPool(ExecuteSessionService executeSessionService,  @Value("${hms-mirror.concurrency.max-threads}") Integer value) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        log.info("Setting up metadataThreadPool with max threads: {}", value);

        executor.setCorePoolSize(value);
        executor.setMaxPoolSize(value);

        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.setThreadNamePrefix("metadata-");
        executor.initialize();
        return executor;
    }

    @Bean("reportingThreadPool")
    @Order(20)
    public TaskExecutor reportingThreadPool() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        log.info("Setting up reportingThreadPool with max threads: 1");

        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);

        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.setThreadNamePrefix("reporting-");
        executor.initialize();
        return executor;
    }

    @Bean("executionThreadPool")
    @Order(20)
    public TaskExecutor executionThreadPool() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // We only want 1 running at a time.
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.setThreadNamePrefix("execution-");
        executor.initialize();
        return executor;
    }

}
