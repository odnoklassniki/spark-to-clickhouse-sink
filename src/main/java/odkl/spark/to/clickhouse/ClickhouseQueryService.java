package odkl.spark.to.clickhouse;

import java.util.Map;

public interface ClickhouseQueryService {
    /**
     * Finds an active server for each shard.
     * @param clusterName Cluster name.
     * @return Map: shardNum -> random active server from sharNum. [1, n] range is expected for a cluster with n shards.
     */
    Map<Integer, String> findActiveServersForEachShard(String clusterName);
}
