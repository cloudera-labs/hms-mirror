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

    private static final int ORDER = 20;
    private static final int DEFAULT_QUEUE_CAPACITY = 1000;
    private static final int SINGLE_THREAD_POOL_SIZE = 1;

    private ThreadPoolTaskExecutor createThreadPool(String threadNamePrefix, int corePoolSize, int maxPoolSize) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(DEFAULT_QUEUE_CAPACITY);
        executor.setThreadNamePrefix(threadNamePrefix);
        executor.initialize();
        return executor;
    }

    @Bean("jobThreadPool")
    @Order(ORDER)
    @ConditionalOnProperty(name = "hms-mirror.concurrency.max-threads")
    public TaskExecutor jobThreadPool(ExecuteSessionService executeSessionService, 
                                    @Value("${hms-mirror.concurrency.max-threads}") Integer value) {
        log.info("Setting up jobThreadPool with max threads: {}", value);
        return createThreadPool("job-", value, value);
    }

    @Bean("metadataThreadPool")
    @Order(ORDER)
    @ConditionalOnProperty(name = "hms-mirror.concurrency.max-threads")
    public TaskExecutor metadataThreadPool(ExecuteSessionService executeSessionService, 
                                         @Value("${hms-mirror.concurrency.max-threads}") Integer value) {
        log.info("Setting up metadataThreadPool with max threads: {}", value);
        return createThreadPool("metadata-", value, value);
    }

    @Bean("reportingThreadPool")
    @Order(ORDER)
    public TaskExecutor reportingThreadPool() {
        log.info("Setting up reportingThreadPool with max threads: {}", SINGLE_THREAD_POOL_SIZE);
        return createThreadPool("reporting-", SINGLE_THREAD_POOL_SIZE, SINGLE_THREAD_POOL_SIZE);
    }

    @Bean("executionThreadPool")
    @Order(ORDER)
    public TaskExecutor executionThreadPool() {
        log.info("Setting up executionThreadPool with max threads: {}", SINGLE_THREAD_POOL_SIZE);
        return createThreadPool("execution-", SINGLE_THREAD_POOL_SIZE, SINGLE_THREAD_POOL_SIZE);
    }
}
