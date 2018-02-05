package com.hansight.hanstreaming;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.queryablestate.client.VoidNamespace;
import org.apache.flink.queryablestate.client.VoidNamespaceSerializer;
import org.apache.flink.queryablestate.exceptions.UnknownKeyOrNamespaceException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.minicluster.FlinkMiniCluster;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by liujia on 2018/2/2.
 */
public class Test {

    private static final FiniteDuration TEST_TIMEOUT = new FiniteDuration(10000L, TimeUnit.SECONDS);

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);
    private final ScheduledExecutor executor = new ScheduledExecutorServiceAdapter(executorService);

    private static final int NUM_TMS = 1;
    private static final int NUM_SLOTS_PER_TM = 4;

    private QueryableStateClient client;
    private LocalFlinkMiniCluster cluster;

    private static final int maxParallelism = 4;

    @org.junit.Test
    public void testAggregatingState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(maxParallelism);
        // Very important, because cluster is shared between tests and we
        // don't explicitly check that all slots are available before
        // submitting.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));

        DataStream<Tuple2<Integer, Long>> source = env.addSource(new TestAscendingValueSource(1000));

        final AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> aggrStateDescriptor =
                new AggregatingStateDescriptor<>("aggregates", new SumAggr(), String.class);
        aggrStateDescriptor.setQueryable("aggr-queryable");

        source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
            private static final long serialVersionUID = 8470749712274833552L;

            @Override
            public Integer getKey(Tuple2<Integer, Long> value) {
                return value.f0;
            }
        }).transform(
                "TestAggregatingOperator",
                BasicTypeInfo.STRING_TYPE_INFO,
                new AggregatingTestOperator(aggrStateDescriptor)
        );

        env.execute();
    }

    @org.junit.Test
    public void testJob() throws JobExecutionException {
        int proxyPortRangeStart = 6124;
        int serverPortRangeStart = 6139;

        Configuration config = new Configuration();
        config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 6124);
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
        config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 4);
        config.setInteger(QueryableStateOptions.CLIENT_NETWORK_THREADS, 1);
        config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 1);
        config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 1);
//        config.setString(QueryableStateOptions.PROXY_PORT_RANGE, proxyPortRangeStart + "-" + (proxyPortRangeStart + NUM_TMS));
//        config.setString(QueryableStateOptions.SERVER_PORT_RANGE, serverPortRangeStart + "-" + (serverPortRangeStart + NUM_TMS));

        FlinkMiniCluster flinkCluster = new LocalFlinkMiniCluster(config, false);
        try {
            flinkCluster.start(true);

            StreamExecutionEnvironment env = StreamExecutionEnvironment
                    .createRemoteEnvironment("localhost", 6124, 4);
            env.setStateBackend(new MemoryStateBackend());

            DataStream<Tuple2<Integer, Long>> source = env.addSource(new TestAscendingValueSource(1000));

            final AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> aggrStateDescriptor =
                    new AggregatingStateDescriptor<>("aggregates", new SumAggr(), String.class);
            aggrStateDescriptor.setQueryable("aggr-queryable");

            source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
                private static final long serialVersionUID = 8470749712274833552L;

                @Override
                public Integer getKey(Tuple2<Integer, Long> value) {
                    return value.f0;
                }
            }).transform(
                    "TestAggregatingOperator",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    new AggregatingTestOperator(aggrStateDescriptor)
            );

            JobGraph jobGraph = env.getStreamGraph().getJobGraph();

            System.out.println("[info] Job ID: " + jobGraph.getJobID());
            System.out.println();

            flinkCluster.submitJobAndWait(jobGraph, false);
        } finally {
            flinkCluster.stop();
            flinkCluster.awaitTermination();
        }
    }

    @org.junit.Test
    public void testQuery() throws Exception {
        client = new QueryableStateClient("localhost", 9069);
        final AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> aggrStateDescriptor =
                new AggregatingStateDescriptor<>("aggregates", new SumAggr(), String.class);
        aggrStateDescriptor.setQueryable("aggr-queryable");

        JobID jobID = JobID.fromHexString("b6db9df006c185624c90cd6017a8151a");
        CompletableFuture<AggregatingState<Tuple2<Integer, Long>, String>> kvState = client.getKvState(jobID,
                "aggr-queryable",
                1, BasicTypeInfo.INT_TYPE_INFO,
                aggrStateDescriptor);

        String result = kvState.get().get();

        System.out.println(result);
    }

    /**
     * Test source producing (key, 0)..(key, maxValue) with key being the sub
     * task index.
     *
     * <p>After all tuples have been emitted, the source waits to be cancelled
     * and does not immediately finish.
     */
    private static class TestAscendingValueSource extends RichParallelSourceFunction<Tuple2<Integer, Long>> {

        private static final long serialVersionUID = 1459935229498173245L;

        private final long maxValue;
        private volatile boolean isRunning = true;

        TestAscendingValueSource(long maxValue) {
            Preconditions.checkArgument(maxValue >= 0);
            this.maxValue = maxValue;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
            // f0 => key
            int key = getRuntimeContext().getIndexOfThisSubtask();
            Tuple2<Integer, Long> record = new Tuple2<>(key, 0L);

            long currentValue = 0;
            while (isRunning && currentValue <= maxValue) {
                synchronized (ctx.getCheckpointLock()) {
                    record.f1 = currentValue;
                    ctx.collect(record);
                }

                currentValue++;
            }

            while (isRunning) {
                synchronized (this) {
                    wait();
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;

            synchronized (this) {
                notifyAll();
            }
        }
    }

    /**
     * An operator that uses {@link AggregatingState}.
     *
     * <p>The operator exists for lack of possibility to get an
     * {@link AggregatingState} from the {@link org.apache.flink.api.common.functions.RuntimeContext}.
     * If this were not the case, we could have a {@link ProcessFunction}.
     */
    private static class AggregatingTestOperator
            extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<Tuple2<Integer, Long>, String> {

        private static final long serialVersionUID = 1L;

        private final AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> stateDescriptor;
        private transient AggregatingState<Tuple2<Integer, Long>, String> state;

        AggregatingTestOperator(AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> stateDesc) {
            this.stateDescriptor = stateDesc;
        }

        @Override
        public void open() throws Exception {
            super.open();
            this.state = getKeyedStateBackend().getPartitionedState(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    stateDescriptor);
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, Long>> element) throws Exception {
            state.add(element.getValue());
        }
    }

    /**
     * Test {@link AggregateFunction} concatenating the already stored string with the long passed as argument.
     */
    private static class SumAggr implements AggregateFunction<Tuple2<Integer, Long>, String, String> {

        private static final long serialVersionUID = -6249227626701264599L;

        @Override
        public String createAccumulator() {
            return "0";
        }

        @Override
        public String add(Tuple2<Integer, Long> value, String accumulator) {
            long acc = Long.valueOf(accumulator);
            acc += value.f1;
            return Long.toString(acc);
        }

        @Override
        public String getResult(String accumulator) {
            return accumulator;
        }

        @Override
        public String merge(String a, String b) {
            return Long.toString(Long.valueOf(a) + Long.valueOf(b));
        }
    }



    /////				General Utility Methods				//////

    /**
     * A wrapper of the job graph that makes sure to cancel the job and wait for
     * termination after the execution of every test.
     */
    private class AutoCancellableJob implements AutoCloseable {

        private final FlinkMiniCluster cluster;
        private final Deadline deadline;
        private final JobGraph jobGraph;

        private final JobID jobId;
        private final CompletableFuture<TestingJobManagerMessages.JobStatusIs> cancellationFuture;

        AutoCancellableJob(final FlinkMiniCluster cluster, final StreamExecutionEnvironment env, final Deadline deadline) {
            Preconditions.checkNotNull(env);

            this.cluster = Preconditions.checkNotNull(cluster);
            this.jobGraph = env.getStreamGraph().getJobGraph();
            this.deadline = Preconditions.checkNotNull(deadline);

            this.jobId = jobGraph.getJobID();
            this.cancellationFuture = notifyWhenJobStatusIs(jobId, JobStatus.CANCELED, deadline);
        }

        JobGraph getJobGraph() {
            return jobGraph;
        }

        JobID getJobId() {
            return jobId;
        }

        @Override
        public void close() throws Exception {
            // Free cluster resources
            if (jobId != null) {
                cluster.getLeaderGateway(deadline.timeLeft())
                        .ask(new JobManagerMessages.CancelJob(jobId), deadline.timeLeft())
                        .mapTo(ClassTag$.MODULE$.<JobManagerMessages.CancellationSuccess>apply(JobManagerMessages.CancellationSuccess.class));

                cancellationFuture.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private CompletableFuture<TestingJobManagerMessages.JobStatusIs> notifyWhenJobStatusIs(
            final JobID jobId, final JobStatus status, final Deadline deadline) {

        return FutureUtils.toJava(
                cluster.getLeaderGateway(deadline.timeLeft())
                        .ask(new TestingJobManagerMessages.NotifyWhenJobStatus(jobId, status), deadline.timeLeft())
                        .mapTo(ClassTag$.MODULE$.<TestingJobManagerMessages.JobStatusIs>apply(TestingJobManagerMessages.JobStatusIs.class)));
    }

    private static <K, S extends State, V> CompletableFuture<S> getKvState(
            final Deadline deadline,
            final QueryableStateClient client,
            final JobID jobId,
            final String queryName,
            final K key,
            final TypeInformation<K> keyTypeInfo,
            final StateDescriptor<S, V> stateDescriptor,
            final boolean failForUnknownKeyOrNamespace,
            final ScheduledExecutor executor) throws InterruptedException {

        final CompletableFuture<S> resultFuture = new CompletableFuture<>();
        getKvStateIgnoringCertainExceptions(
                deadline, resultFuture, client, jobId, queryName, key, keyTypeInfo,
                stateDescriptor, failForUnknownKeyOrNamespace, executor);
        return resultFuture;
    }

    private static <K, S extends State, V> void getKvStateIgnoringCertainExceptions(
            final Deadline deadline,
            final CompletableFuture<S> resultFuture,
            final QueryableStateClient client,
            final JobID jobId,
            final String queryName,
            final K key,
            final TypeInformation<K> keyTypeInfo,
            final StateDescriptor<S, V> stateDescriptor,
            final boolean failForUnknownKeyOrNamespace,
            final ScheduledExecutor executor) throws InterruptedException {

        if (!resultFuture.isDone()) {
            Thread.sleep(100L);
            CompletableFuture<S> expected = client.getKvState(jobId, queryName, key, keyTypeInfo, stateDescriptor);
            expected.whenCompleteAsync((result, throwable) -> {
                if (throwable != null) {
                    if (
                            throwable.getCause() instanceof CancellationException ||
                                    throwable.getCause() instanceof AssertionError ||
                                    (failForUnknownKeyOrNamespace && throwable.getCause() instanceof UnknownKeyOrNamespaceException)
                            ) {
                        resultFuture.completeExceptionally(throwable.getCause());
                    } else if (deadline.hasTimeLeft()) {
                        try {
                            getKvStateIgnoringCertainExceptions(
                                    deadline, resultFuture, client, jobId, queryName, key, keyTypeInfo,
                                    stateDescriptor, failForUnknownKeyOrNamespace, executor);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } else {
                    resultFuture.complete(result);
                }
            }, executor);

            resultFuture.whenComplete((result, throwable) -> expected.cancel(false));
        }
    }

    /**
     * Retry a query for state for keys between 0 and {@link #maxParallelism} until
     * <tt>expected</tt> equals the value of the result tuple's second field.
     */
    private void executeValueQuery(
            final Deadline deadline,
            final QueryableStateClient client,
            final JobID jobId,
            final String queryableStateName,
            final ValueStateDescriptor<Tuple2<Integer, Long>> stateDescriptor,
            final long expected) throws Exception {

        for (int key = 0; key < maxParallelism; key++) {
            boolean success = false;
            while (deadline.hasTimeLeft() && !success) {
                CompletableFuture<ValueState<Tuple2<Integer, Long>>> future = getKvState(
                        deadline,
                        client,
                        jobId,
                        queryableStateName,
                        key,
                        BasicTypeInfo.INT_TYPE_INFO,
                        stateDescriptor,
                        false,
                        executor);

                Tuple2<Integer, Long> value = future.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS).value();

                assertEquals("Key mismatch", key, value.f0.intValue());
                if (expected == value.f1) {
                    success = true;
                } else {
                    // Retry
                    Thread.sleep(50L);
                }
            }

            assertTrue("Did not succeed query", success);
        }
    }


    @After
    public void tearDown() {
        client.shutdown();

        cluster.stop();
        cluster.awaitTermination();
    }
}
