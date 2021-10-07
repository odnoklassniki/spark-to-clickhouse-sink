package odkl.spark.to.clickhouse;

import com.google.common.base.Preconditions;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedRunnable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * The class knows how to write to Clickhouse cluster directly to local tables (MergeTree family) instead
 * of writing via Clickhouse Distributed Engine
 * (writes via Distributed tables had been proven to be a very bad idea).
 * You could think about the writer as thick-write-only-client for Clickhouse.
 * The writer splits all data on partitions based on {@link SparkToClickhouseWriterSettings#getShardingColumn()}
 * (similar to Distributed Table Engine) and
 * {@link SparkToClickhouseWriterSettings#getShardInsertParallelism()}. The total number of output partitions is
 * numberOfClickhouseShards * {@link SparkToClickhouseWriterSettings#getShardInsertParallelism()}.
 * Then each partition gets written into its own shard.
 * <br/><br/>
 * DEDUPLICATION.
 * <br/>
 * The writer can also write data w/o duplicates from repeatable source. For example it can be very useful in achieving
 * EOS semantic when Kafka-Clickhouse sink is needed.
 * To do that you need just provide {@link SparkToClickhouseWriterSettings#getDeduplicationSortingKey()}.
 * The sorting key will be used to sort rows inside a partition.
 * It could be any columns which lead to a stable sorting inside a partition.
 * The mechanism relies on Clickhouse deduplication for same data blocks.
 * See: https://clickhouse.tech/docs/en/operations/settings/settings/#settings-insert-deduplicate.
 */
public class SparkToClickhouseWriter {
    private static final int MAX_ATTEMPTS_BEFORE_FAIL = 3;
    private static final Duration PAUSE_BETWEEN_SEQUENTIAL_RETRIES = Duration.ofSeconds(20);
    private static final String CLICKHOUSE_JDBC_URL_TEMPLATE = "jdbc:clickhouse://%hosts%/%database%?log_queries=1";

    private final ClickhouseJdbcConfig chConfig;
    private final SparkToClickhouseWriterSettings settings;
    private final ClickhouseQueryService clickhouseQueryService;

    public SparkToClickhouseWriter(ClickhouseJdbcConfig chConfig,
                                   SparkToClickhouseWriterSettings settings,
                                   ClickhouseQueryService clickhouseQueryService) {
        this.chConfig = chConfig;
        this.settings = settings;
        this.clickhouseQueryService = clickhouseQueryService;
    }

    /**
     * Saves provided DataFrame to Clickhouse.
     * @param df Data frame.
     */
    public void save(Dataset<Row> df) {
        processDfWithRetries(df, this::saveDataset);
    }

    private void processDfWithRetries(Dataset<Row> df, CheckedCallable<Dataset<Row>> op) {
        Dataset<Row> cachedDf = df.cache();
        try {
            RetryPolicy<Void> retryPolicy = new RetryPolicy<Void>()
                    .handle(Exception.class)
                    .withDelay(PAUSE_BETWEEN_SEQUENTIAL_RETRIES)
                    .withMaxRetries(MAX_ATTEMPTS_BEFORE_FAIL);

            CheckedRunnable checkedRunnable = () -> op.call(df);
            Failsafe.with(retryPolicy).run(checkedRunnable);
        } finally {
            cachedDf.unpersist();
        }
    }

    private void saveDataset(final Dataset<Row> df) throws InterruptedException {
        Map<Integer, String> activeServersForEachShard = clickhouseQueryService
                .findActiveServersForEachShard(chConfig.getClusterName());

        checkShardsNumbersAreValid(activeServersForEachShard.keySet());

        // Run in parallel.
        int shardsNum = activeServersForEachShard.size();

        List<Callable<Boolean>> tasks = activeServersForEachShard.entrySet().stream()
                .map(shardNumToServer -> (Callable<Boolean>) () ->
                        saveDatasetIntoShard(df, shardsNum, shardNumToServer))
                .collect(Collectors.toList());

        runInParallel(tasks);
    }

    private void runInParallel(List<Callable<Boolean>> tasks) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(tasks.size(),
                new NamedDefaultThreadFactory("saveToChShards"));
        try {
            List<Future<Boolean>> futures = executorService.invokeAll(tasks);

            futures.forEach(f -> {
                try {
                    f.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            executorService.shutdown();
        }
    }

    private void checkShardsNumbersAreValid(Set<Integer> shards) {
        Preconditions.checkArgument(shards.size() > 0, "Empty shards.");
        String error = "It is expected that shards are in range [0, size-1] and sharding algorithm depend on that. " +
                "Shards=" + shards.stream().sorted().collect(Collectors.toList());
        Preconditions.checkArgument(shards.stream().mapToInt(Integer::intValue)
                .min().getAsInt() == 1, error);
        Preconditions.checkArgument(shards.stream().mapToInt(Integer::intValue)
                .max().getAsInt() == shards.size(), error);
    }

    private Boolean saveDatasetIntoShard(final Dataset<Row> df,
                                         int shardsNum,
                                         Map.Entry<Integer, String> shardNumToServer) {
        Column shardCondition = settings.getShardingColumn()
                .mod(shardsNum)
                .equalTo(shardNumToServer.getKey() - 1); // -1 in order to convert [1, n] range to [0, n-1].

        // We do filtration n times (for each shard).
        // It seems we could do it once.
        // Another possible option is to do the following logic
        // df.repartition(shardsNum * shardInsertParallelism(), shardingColumn())
        //   .sortWithinPartitions(sortingDeduplicationKey);
        //   .foreachPartition(new MyForeachPartitionFunction(...))
        // But you will have to reimplement logic of saving to CH inside MyForeachPartitionFunction.
        Dataset<Row> shardDf = df
                .filter(shardCondition)
                .repartition(settings.getShardInsertParallelism(), settings.getShardingColumn());

        if (isDeduplicationEnabled()) {
            shardDf = shardDf.sortWithinPartitions(listToSeq(settings.getDeduplicationSortingKey()));
        }

        // It doesn't support ARRAY-columns.
        processDfWithRetries(shardDf, (df0) ->
                df0.write()
                        .mode(SaveMode.Append)
                        .jdbc(getUrlForHost(shardNumToServer.getValue()),
                                chConfig.getDatabase() + "." + chConfig.getLocalTable(),
                                jdbcWriteOptions())
        );

        return true;
    }

    private static Seq<Column> listToSeq(List<Column> list) {
        return JavaConverters.iterableAsScalaIterableConverter(list).asScala().toSeq();
    }

    private boolean isDeduplicationEnabled() {
        return !settings.getDeduplicationSortingKey().isEmpty();
    }

    private Properties jdbcWriteOptions() {
        Properties properties = new Properties();
        properties.put("driver", chConfig.getDriver());
        properties.put("autocommit", chConfig.getAutoCommit());
        properties.put("user", chConfig.getUser());
        properties.put("password", chConfig.getPassword());
        properties.put("batchsize", String.valueOf(chConfig.getBatchSize()));
        if (isDeduplicationEnabled()) {
            properties.put(ClickHouseQueryParam.INSERT_DEDUPLICATE.getKey(), "1");
        }
        return properties;
    }

    private String getUrlForHost(String host) {
        return CLICKHOUSE_JDBC_URL_TEMPLATE
                .replace("%hosts%", host + ":" + chConfig.getPort())
                .replace("%database%", chConfig.getDatabase());
    }

    /**
     * Default thread factory which allows to provide thread prefix.
     * Copy-paste of java.util.concurrent.Executors.DefaultThreadFactory.
     */
    private static class NamedDefaultThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        NamedDefaultThreadFactory(String prefix) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = prefix + "-pool-" +
                    poolNumber.getAndIncrement() +
                    "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

    @FunctionalInterface
    private interface CheckedCallable<T> {
        void call(T t) throws Exception;
    }
}
