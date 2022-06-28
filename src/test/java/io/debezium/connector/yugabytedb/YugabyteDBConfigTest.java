package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.testcontainers.containers.YugabyteYSQLContainer;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;

public class YugabyteDBConfigTest extends YugabyteDBTestBase {
  private final static Logger LOGGER = Logger.getLogger(YugabyteDBConnectorIT.class);

    private static YugabyteYSQLContainer ybContainer;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        ybContainer = TestHelper.getYbContainer();
        ybContainer.start();

        TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433));
        TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();
    }

    @After
    public void after() throws Exception {
        stopConnector();
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ybContainer.stop();
    }

    private void insertRecords(int numOfRowsToBeInserted) throws Exception {
        String formatInsertString = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < numOfRowsToBeInserted; i++) {
                TestHelper.execute(String.format(formatInsertString, i));
            }
        }).exceptionally(throwable -> {
            throw new RuntimeException(throwable);
        }).get();
    }

    private void verifyRecordCount(int recordsCount) {
        int totalConsumedRecords = 0;
        while (totalConsumedRecords < recordsCount) {
            int consumed = super.consumeAvailableRecords(record -> {
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                LOGGER.debug("Consumed " + totalConsumedRecords + " records");
            }
        }
        assertEquals(recordsCount, totalConsumedRecords);
    }

    // This verifies that the connector should not be running if a wrong table.include.list is provided
    @Test
    public void shouldThrowExceptionWithWrongIncludeList() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "all_types");

        // Create another table which will not be a part of the DB stream ID
        TestHelper.execute("CREATE TABLE not_part_of_stream (id INT PRIMARY KEY);");

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.all_types,public.not_part_of_stream", dbStreamId);

        // This should throw a DebeziumException saying the table not_part_of_stream is not a part of stream ID
        start(YugabyteDBConnector.class, configBuilder.build(), (success, msg, error) -> {
            assertFalse(success);
            // assertThat(success).isFalse();
            assertTrue(error instanceof DebeziumException);
            // assertThat(error).isInstanceOf(DebeziumException.class);

            assertTrue(error.getMessage().contains("is not a part of the stream ID " + dbStreamId));
            // assertThat(error.getMessage().contains("is not a part of the stream ID " + dbStreamId)).isTrue();
        });

        assertConnectorNotRunning();
    }

    // This test verifies that the connector configuration works properly even if there are tables of the same name
    // in another database
    @Test
    public void shouldWorkWithSameNameTablePresentInAnotherDatabase() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");

        // Create a same table in another database
        // This is to ensure that when the yb-client returns all the tables, then the YugabyteDBConnector
        // is filtering them properly
        String createNewTableStatement = "CREATE TABLE t1 (id INT PRIMARY KEY, first_name TEXT NOT NULL, last_name VARCHAR(40), hours DOUBLE PRECISION);";
        
        // The name of the secondary database is secondary_database
        TestHelper.createTableInSecondaryDatabase(createNewTableStatement);

        // The config builder returns a default config with the database as "postgres"
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

        start(YugabyteDBConnector.class, configBuilder.build());

        awaitUntilConnectorIsReady();

        int recordsCount = 10;
        insertRecords(recordsCount);

        CompletableFuture.runAsync(() -> verifyRecordCount(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }
  
}
