/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.RecordOrEventCollectingResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.NoOpCheckpointResponder;
import org.apache.flink.runtime.taskmanager.NoOpTaskOperatorEventGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingUserCodeClassLoader;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/** Mock {@link Environment}. */
public class StreamMockEnvironment implements Environment {

    private final TaskInfo taskInfo;

    private final MemoryManager memManager;

    private final SharedResources sharedResources;

    private final IOManager ioManager;

    private final InputSplitProvider inputSplitProvider;

    private final Configuration jobConfiguration;

    private final Configuration taskConfiguration;

    private final List<IndexedInputGate> inputs;

    private List<ResultPartitionWriter> outputs;

    private final JobID jobID;

    private final ExecutionAttemptID executionAttemptID;

    private final BroadcastVariableManager bcVarManager = new BroadcastVariableManager();

    private final AccumulatorRegistry accumulatorRegistry;

    private final TaskKvStateRegistry kvStateRegistry;

    private final int bufferSize;

    private final ExecutionConfig executionConfig;

    private final TaskStateManager taskStateManager;

    private final GlobalAggregateManager aggregateManager;

    private final UserCodeClassLoader userCodeClassLoader =
            TestingUserCodeClassLoader.newBuilder().build();

    private final boolean collectNetworkEvents;

    @Nullable private Consumer<Throwable> externalExceptionHandler;

    private TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);

    private TaskManagerRuntimeInfo taskManagerRuntimeInfo = new TestingTaskManagerRuntimeInfo();

    private TaskMetricGroup taskMetricGroup =
            UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();

    private CheckpointResponder checkpointResponder = NoOpCheckpointResponder.INSTANCE;

    public StreamMockEnvironment(
            Configuration jobConfig,
            Configuration taskConfig,
            ExecutionConfig executionConfig,
            long memorySize,
            MockInputSplitProvider inputSplitProvider,
            int bufferSize,
            TaskStateManager taskStateManager) {
        this(
                new JobID(),
                createExecutionAttemptId(),
                jobConfig,
                taskConfig,
                executionConfig,
                memorySize,
                inputSplitProvider,
                bufferSize,
                taskStateManager,
                false);
    }

    public StreamMockEnvironment(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            Configuration jobConfig,
            Configuration taskConfig,
            ExecutionConfig executionConfig,
            long offHeapMemorySize,
            MockInputSplitProvider inputSplitProvider,
            int bufferSize,
            TaskStateManager taskStateManager,
            boolean collectNetworkEvents) {

        this.jobID = jobID;
        this.executionAttemptID = executionAttemptID;

        int subtaskIndex = executionAttemptID.getExecutionVertexId().getSubtaskIndex();
        this.taskInfo =
                new TaskInfo(
                        "", /* task name */
                        1, /* num key groups / max parallelism */
                        subtaskIndex, /* index of this subtask */
                        1, /* num subtasks */
                        executionAttemptID.getAttemptNumber() /* attempt number */);
        this.jobConfiguration = jobConfig;
        this.taskConfiguration = taskConfig;
        this.inputs = new LinkedList<>();
        this.outputs = new LinkedList<ResultPartitionWriter>();
        this.memManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(offHeapMemorySize).build();
        this.sharedResources = new SharedResources();
        this.ioManager = new IOManagerAsync();
        this.taskStateManager = Preconditions.checkNotNull(taskStateManager);
        this.aggregateManager = new TestGlobalAggregateManager();
        this.inputSplitProvider = inputSplitProvider;
        this.bufferSize = bufferSize;

        this.executionConfig = executionConfig;
        this.accumulatorRegistry = new AccumulatorRegistry(jobID, getExecutionId());

        KvStateRegistry registry = new KvStateRegistry();
        this.kvStateRegistry =
                registry.createTaskRegistry(
                        jobID, executionAttemptID.getExecutionVertexId().getJobVertexId());
        this.collectNetworkEvents = collectNetworkEvents;
    }

    public StreamMockEnvironment(
            Configuration jobConfig,
            Configuration taskConfig,
            long memorySize,
            MockInputSplitProvider inputSplitProvider,
            int bufferSize,
            TaskStateManager taskStateManager) {

        this(
                jobConfig,
                taskConfig,
                new ExecutionConfig(),
                memorySize,
                inputSplitProvider,
                bufferSize,
                taskStateManager);
    }

    public void addInputGate(IndexedInputGate gate) {
        inputs.add(gate);
    }

    public <T> void addOutput(
            final Collection<Object> outputList, final TypeSerializer<T> serializer) {
        addOutput(
                new RecordOrEventCollectingResultPartitionWriter<T>(
                        outputList, serializer, collectNetworkEvents));
    }

    public void addOutput(ResultPartitionWriter resultPartitionWriter) {
        try {
            outputs.add(resultPartitionWriter);
        } catch (Throwable t) {
            t.printStackTrace();
            fail(t.getMessage());
        }
    }

    public void setOutputs(List<ResultPartitionWriter> outputs) {
        this.outputs = outputs;
    }

    public void setExternalExceptionHandler(Consumer<Throwable> externalExceptionHandler) {
        this.externalExceptionHandler = externalExceptionHandler;
    }

    @Override
    public Configuration getTaskConfiguration() {
        return this.taskConfiguration;
    }

    @Override
    public MemoryManager getMemoryManager() {
        return this.memManager;
    }

    @Override
    public SharedResources getSharedResources() {
        return this.sharedResources;
    }

    @Override
    public IOManager getIOManager() {
        return this.ioManager;
    }

    @Override
    public ExecutionConfig getExecutionConfig() {
        return this.executionConfig;
    }

    @Override
    public JobID getJobID() {
        return this.jobID;
    }

    @Override
    public Configuration getJobConfiguration() {
        return this.jobConfiguration;
    }

    @Override
    public InputSplitProvider getInputSplitProvider() {
        return this.inputSplitProvider;
    }

    @Override
    public TaskInfo getTaskInfo() {
        return this.taskInfo;
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return userCodeClassLoader;
    }

    @Override
    public Map<String, Future<Path>> getDistributedCacheEntries() {
        return Collections.emptyMap();
    }

    @Override
    public ResultPartitionWriter getWriter(int index) {
        return outputs.get(index);
    }

    @Override
    public ResultPartitionWriter[] getAllWriters() {
        return outputs.toArray(new ResultPartitionWriter[outputs.size()]);
    }

    @Override
    public IndexedInputGate getInputGate(int index) {
        return inputs.get(index);
    }

    @Override
    public IndexedInputGate[] getAllInputGates() {
        return inputs.toArray(new IndexedInputGate[0]);
    }

    @Override
    public TaskEventDispatcher getTaskEventDispatcher() {
        return taskEventDispatcher;
    }

    @Override
    public JobVertexID getJobVertexId() {
        return new JobVertexID(new byte[16]);
    }

    @Override
    public ExecutionAttemptID getExecutionId() {
        return executionAttemptID;
    }

    @Override
    public BroadcastVariableManager getBroadcastVariableManager() {
        return this.bcVarManager;
    }

    @Override
    public TaskStateManager getTaskStateManager() {
        return taskStateManager;
    }

    @Override
    public GlobalAggregateManager getGlobalAggregateManager() {
        return aggregateManager;
    }

    @Override
    public AccumulatorRegistry getAccumulatorRegistry() {
        return accumulatorRegistry;
    }

    @Override
    public TaskKvStateRegistry getTaskKvStateRegistry() {
        return kvStateRegistry;
    }

    @Override
    public ExternalResourceInfoProvider getExternalResourceInfoProvider() {
        return ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES;
    }

    @Override
    public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics) {}

    @Override
    public void acknowledgeCheckpoint(
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot subtaskState) {
        taskStateManager.reportTaskStateSnapshots(
                new CheckpointMetaData(checkpointId, 0L), checkpointMetrics, subtaskState, null);
    }

    @Override
    public void declineCheckpoint(long checkpointId, CheckpointException checkpointException) {
        checkpointResponder.declineCheckpoint(
                jobID, executionAttemptID, checkpointId, checkpointException);
    }

    @Override
    public TaskOperatorEventGateway getOperatorCoordinatorEventGateway() {
        return new NoOpTaskOperatorEventGateway();
    }

    @Override
    public void failExternally(Throwable cause) {
        if (externalExceptionHandler != null) {
            externalExceptionHandler.accept(cause);
        }
    }

    @Override
    public TaskManagerRuntimeInfo getTaskManagerInfo() {
        return this.taskManagerRuntimeInfo;
    }

    public void setTaskManagerInfo(TaskManagerRuntimeInfo taskManagerRuntimeInfo) {
        this.taskManagerRuntimeInfo = taskManagerRuntimeInfo;
    }

    @Override
    public TaskMetricGroup getMetricGroup() {
        return this.taskMetricGroup;
    }

    public void setTaskMetricGroup(TaskMetricGroup taskMetricGroup) {
        this.taskMetricGroup = taskMetricGroup;
    }

    public void setCheckpointResponder(CheckpointResponder checkpointResponder) {
        this.checkpointResponder = checkpointResponder;
    }
}
