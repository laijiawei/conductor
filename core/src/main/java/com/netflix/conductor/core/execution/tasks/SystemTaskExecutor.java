/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.core.execution.tasks;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.core.utils.SemaphoreUtil;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the threadpool used by system task workers for execution.
 */
public class SystemTaskExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SystemTaskExecutor.class);

    private final int callbackTime;
    private final QueueDAO queueDAO;

    ExecutionConfig defaultExecutionConfig;
    private final WorkflowExecutor workflowExecutor;
    private final Configuration config;

    ConcurrentHashMap<String, ExecutionConfig> queueExecutionConfigMap = new ConcurrentHashMap<>();

    SystemTaskExecutor(WorkflowExecutor workflowExecutor, QueueDAO queueDAO, Configuration config) {
        this.config = config;
        int threadCount = config.getIntProperty("workflow.system.task.worker.thread.count", 10);
        this.callbackTime = config.getIntProperty("workflow.system.task.worker.callback.seconds", 30);

        String threadNameFormat = "system-task-worker-%d";
        this.defaultExecutionConfig = new ExecutionConfig(threadCount, threadNameFormat);
        this.workflowExecutor = workflowExecutor;
        this.queueDAO = queueDAO;

        LOGGER.info("Initialized the SystemTaskExecutor with {} threads and callback time: {} seconds", threadCount,
            callbackTime);
    }

    void pollAndExecute(String queueName) {
        // get the remaining capacity of worker queue to prevent queue full exception
        ExecutionConfig executionConfig = getExecutionConfig(queueName);
        SemaphoreUtil semaphoreUtil = executionConfig.getSemaphoreUtil();
        ExecutorService executorService = executionConfig.getExecutorService();
        String taskName = QueueUtils.getTaskType(queueName);

        if (!semaphoreUtil.canProcess()) {
            // no available permits, do not poll
            return;
        }
        try {
            List<String> polledTaskIds = queueDAO.pop(queueName, 1, 200);
            Monitors.recordTaskPoll(queueName);
            LOGGER.debug("Polling queue:{}, got {} tasks", queueName, polledTaskIds.size());
            if (polledTaskIds.size() == 1 && StringUtils.isNotBlank(polledTaskIds.get(0))) {
                String taskId = polledTaskIds.get(0);
                LOGGER.debug("Task: {} from queue: {} being sent to the workflow executor", taskId, queueName);
                Monitors.recordTaskPollCount(queueName, "", 1);

                WorkflowSystemTask systemTask = SystemTaskWorkerCoordinator.taskNameWorkflowTaskMapping.get(taskName);
                CompletableFuture<Void> taskCompletableFuture = CompletableFuture.runAsync(() ->
                    workflowExecutor.executeSystemTask(systemTask, taskId, callbackTime), executorService);

                // release permit after processing is complete
                taskCompletableFuture.whenComplete((r,e) -> semaphoreUtil.completeProcessing());
            } else {
                // no task polled, release permit
                semaphoreUtil.completeProcessing();
            }
        } catch (Exception e) {
            // release the permit if exception is thrown during polling, because the thread would not be busy
            semaphoreUtil.completeProcessing();
            Monitors.recordTaskPollError(taskName, "", e.getClass().getSimpleName());
            LOGGER.error("Error polling system task in queue:{}", queueName, e);
        }
    }

    @VisibleForTesting
    ExecutionConfig getExecutionConfig(String taskQueue) {
        if (!QueueUtils.isIsolatedQueue(taskQueue)) {
            return this.defaultExecutionConfig;
        }
        return queueExecutionConfigMap.computeIfAbsent(taskQueue, __ -> this.createExecutionConfig());
    }

    private ExecutionConfig createExecutionConfig() {
        int threadCount = config.getIntProperty("workflow.isolated.system.task.worker.thread.count", 1);
        String threadNameFormat = "isolated-system-task-worker-%d";
        return new ExecutionConfig(threadCount, threadNameFormat);
    }
}
