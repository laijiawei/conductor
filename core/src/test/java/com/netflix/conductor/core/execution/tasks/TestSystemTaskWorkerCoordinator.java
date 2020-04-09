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

import static org.junit.Assert.assertTrue;

import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.QueueDAO;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestSystemTaskWorkerCoordinator {

	public static final String TEST_QUEUE = "test";
	public static final String ISOLATION_CONSTANT = "-iso";

//	@Test
//	public void testPollAndExecuteForIsolatedQueues() {
//		createTaskMapping();
//
//		Configuration configuration = Mockito.mock(Configuration.class);
//		QueueDAO queueDAO = Mockito.mock(QueueDAO.class);
//		Mockito.when(configuration.getIntProperty(Mockito.anyString(), Mockito.anyInt())).thenReturn(10);
//		Mockito.when(queueDAO.pop(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(Collections.singletonList("taskId"));
//		WorkflowExecutor workflowExecutor = Mockito.mock(WorkflowExecutor.class);
//		SystemTaskWorkerCoordinator systemTaskWorkerCoordinator = new SystemTaskWorkerCoordinator(queueDAO, workflowExecutor, configuration);
//
//		systemTaskWorkerCoordinator.pollAndExecute(TEST_QUEUE + ISOLATION_CONSTANT);
//		shutDownExecutors(systemTaskWorkerCoordinator);
//
//		Mockito.verify(workflowExecutor, Mockito.times(1)).executeSystemTask(Mockito.any(), Mockito.anyString(), Mockito.anyInt());
//	}
//
//	@Test
//	public void testPollAndExecuteForTaskQueues() {
//		createTaskMapping();
//
//		Configuration configuration = Mockito.mock(Configuration.class);
//		QueueDAO queueDao = Mockito.mock(QueueDAO.class);
//		Mockito.when(configuration.getIntProperty(Mockito.any(), Mockito.anyInt())).thenReturn(10);
//		Mockito.when(queueDao.pop(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(Collections.singletonList("taskId"));
//		WorkflowExecutor wfE = Mockito.mock(WorkflowExecutor.class);
//		SystemTaskWorkerCoordinator systemTaskWorkerCoordinator = new SystemTaskWorkerCoordinator(queueDao, wfE, configuration);
//
//		systemTaskWorkerCoordinator.pollAndExecute(TEST_QUEUE);
//		shutDownExecutors(systemTaskWorkerCoordinator);
//
//		Mockito.verify(wfE, Mockito.times(1)).executeSystemTask(Mockito.any(), Mockito.anyString(), Mockito.anyInt());
//	}
//
//	private void shutDownExecutors(SystemTaskWorkerCoordinator systemTaskWorkerCoordinator) {
//		systemTaskWorkerCoordinator.defaultExecutionConfig.executorService.shutdown();
//
//		try {
//			systemTaskWorkerCoordinator.defaultExecutionConfig.executorService.awaitTermination(10, TimeUnit.MILLISECONDS);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//
//		systemTaskWorkerCoordinator.queueExecutionConfigMap.values().forEach(e -> {
//			e.getExecutorService().shutdown();
//			try {
//				e.getExecutorService().awaitTermination(10, TimeUnit.MILLISECONDS);
//			} catch (InterruptedException ie) {
//
//			}
//		});
//	}
//
//	@Test
//	public void isSystemTask() {
//		createTaskMapping();
//		assertTrue(SystemTaskWorkerCoordinator.isAsyncSystemTask(TEST_QUEUE + ISOLATION_CONSTANT));
//	}
//
//	@Test
//	public void isSystemTaskNotPresent() {
//		createTaskMapping();
//		Assert.assertFalse(SystemTaskWorkerCoordinator.isAsyncSystemTask(null));
//	}
//
//	private void createTaskMapping() {
//		WorkflowSystemTask mockWorkflowTask = Mockito.mock(WorkflowSystemTask.class);
//		Mockito.when(mockWorkflowTask.getName()).thenReturn(TEST_QUEUE);
//		Mockito.when(mockWorkflowTask.isAsync()).thenReturn(true);
//		SystemTaskWorkerCoordinator.taskNameWorkflowTaskMapping.put(TEST_QUEUE, mockWorkflowTask);
//	}
//
//	@Test
//	public void testGetExecutionConfigForSystemTask() {
//		Configuration configuration = Mockito.mock(Configuration.class);
//		Mockito.when(configuration.getIntProperty(Mockito.anyString(), Mockito.anyInt())).thenReturn(1);
//		SystemTaskWorkerCoordinator systemTaskWorkerCoordinator = new SystemTaskWorkerCoordinator(Mockito.mock(QueueDAO.class), Mockito.mock(WorkflowExecutor.class), configuration);
//		assertEquals(systemTaskWorkerCoordinator.getExecutionConfig("").workerQueue.remainingCapacity(), 1);
//	}
//
//	@Test
//	public void testGetExecutionConfigForIsolatedSystemTask() {
//		Configuration configuration = new SystemPropertiesConfiguration();
//		SystemTaskWorkerCoordinator systemTaskWorkerCoordinator = new SystemTaskWorkerCoordinator(Mockito.mock(QueueDAO.class), Mockito.mock(WorkflowExecutor.class), configuration);
//		assertEquals(systemTaskWorkerCoordinator.getExecutionConfig("test-iso").workerQueue.remainingCapacity(), 100);
//	}
//
//
//	@Test
//	public void testIsFromCoordinatorDomain() {
//		System.setProperty("workflow.system.task.worker.domain","domain");
//		Configuration configuration = new SystemPropertiesConfiguration();
//		SystemTaskWorkerCoordinator systemTaskWorkerCoordinator = new SystemTaskWorkerCoordinator(Mockito.mock(QueueDAO.class), Mockito.mock(WorkflowExecutor.class), configuration);
//		assertTrue(systemTaskWorkerCoordinator.isFromCoordinatorExecutionNameSpace("domain:testTaskType"));
//	}
}
