package odkl.spark.to.clickhouse;

public interface ClickhouseJdbcConfig {
    String getClusterName();

    String getDatabase();

    /**
     * A local table is a table which holds the actual data in opposite to Distributed table.
     * MergeTree family tables is a good example for a local table.
     * @return Table name.
     */
    String getLocalTable();

    String getDriver();

    String getAutoCommit();

    String getUser();

    String getPassword();

    int getBatchSize();

    String getPort();
}
