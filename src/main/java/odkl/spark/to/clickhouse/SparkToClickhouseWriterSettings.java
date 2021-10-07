package odkl.spark.to.clickhouse;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

public class SparkToClickhouseWriterSettings {
    private final Integer shardInsertParallelism;
    private final Column shardingColumn;
    private final List<Column> deduplicationSortingKey;

    public SparkToClickhouseWriterSettings(
            Integer shardInsertParallelism,
            String shardingColumn,
            List<String> sortingKey) {

        this.shardInsertParallelism = shardInsertParallelism;
        this.shardingColumn = abs(hash(col(shardingColumn)));
        this.deduplicationSortingKey = sortingKey != null ?
                sortingKey.stream().map(functions::col).collect(Collectors.toList()) :
                Collections.emptyList();
    }

    public int getShardInsertParallelism() {
        return shardInsertParallelism;
    }

    public Column getShardingColumn() {
        return shardingColumn;
    }

    public List<Column> getDeduplicationSortingKey() {
        return deduplicationSortingKey;
    }
}
