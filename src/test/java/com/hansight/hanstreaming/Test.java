package com.hansight.hanstreaming;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.queryablestate.client.VoidNamespace;
import org.apache.flink.queryablestate.client.VoidNamespaceSerializer;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
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
import org.junit.Assert;

import java.util.concurrent.CompletableFuture;

/**
 * Created by liujia on 2018/2/2.
 */
public class Test {

    private QueryableStateClient client;
    private LocalFlinkMiniCluster cluster;

    private static final int maxParallelism = 4;

    @org.junit.Test
    public void testAggregatingState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(maxParallelism);
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
        Configuration config = new Configuration();
        config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 6124);
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
        config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 4);
        config.setInteger(QueryableStateOptions.CLIENT_NETWORK_THREADS, 1);
        config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 1);
        config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 1);

        cluster = new LocalFlinkMiniCluster(config, false);
        try {
            cluster.start(true);

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

            cluster.submitJobAndWait(jobGraph, false);
        } finally {
            cluster.stop();
            cluster.awaitTermination();
        }
    }

    @org.junit.Test
    public void testQuery() throws Exception {
        client = new QueryableStateClient("localhost", 9069);
        final AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> aggrStateDescriptor =
                new AggregatingStateDescriptor<>("aggregates", new SumAggr(), String.class);
        aggrStateDescriptor.setQueryable("aggr-queryable");

        JobID jobID = JobID.fromHexString("ece5660c0e32a7d9780b8f24cd4fffc6");
        CompletableFuture<AggregatingState<Tuple2<Integer, Long>, String>> kvState = client.getKvState(jobID,
                "aggr-queryable",
                1, BasicTypeInfo.INT_TYPE_INFO,
                aggrStateDescriptor);

        String result = kvState.get().get();

        Assert.assertNotNull(result);
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

    @After
    public void tearDown() {
        client.shutdown();
    }
}
