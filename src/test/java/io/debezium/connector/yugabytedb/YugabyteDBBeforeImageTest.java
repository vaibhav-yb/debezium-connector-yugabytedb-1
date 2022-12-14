package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.YugabyteYSQLContainer;
import org.yb.client.YBClient;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;

public class YugabyteDBBeforeImageTest extends YugabyteDBTestBase {
  private final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBPartitionTest.class);
  private final String formatInsertString =
      "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 12.345);";
  private static YugabyteYSQLContainer ybContainer;
//   private static String masterAddresses;

  @BeforeClass
  public static void beforeClass() throws SQLException {
    //   ybContainer = TestHelper.getYbContainer();
    //   ybContainer.start();

    //   TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433));
    //   TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
    //   masterAddresses = ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100);

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
    //   ybContainer.stop();
  }

  @Test
  public void isBeforeGettingPublished() throws Exception {
      TestHelper.initDB("yugabyte_create_tables.ddl");

      String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", true /* withBeforeImage */);
      Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
      start(YugabyteDBConnector.class, configBuilder.build());

      awaitUntilConnectorIsReady();

      // Insert a record and update it.
      TestHelper.execute(String.format(formatInsertString, 1));
      TestHelper.execute("UPDATE t1 SET first_name='VKVK', hours=56.78 where id = 1;");

      // Consume the records and verify that the records should have the relevant information.
      List<SourceRecord> records = new ArrayList<>();
      CompletableFuture.runAsync(() -> getRecords(records, 2, 20000)).get();

      // The first record is an insert record with before image as null.
      SourceRecord insertRecord = records.get(0);
      assertValueField(insertRecord, "before", null);
      assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

      // The second record will be an update record having a before image.
      SourceRecord updateRecord = records.get(1);
      assertBeforeImage(updateRecord, 1, "Vaibhav", "Kushwaha", 12.345);
      assertAfterImage(updateRecord, 1, "VKVK", "Kushwaha", 56.78);
  }

  @Test
  public void consecutiveSingleShardTransactions() throws Exception {
      TestHelper.initDB("yugabyte_create_tables.ddl");

      String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", true /* withBeforeImage */);
      Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
      start(YugabyteDBConnector.class, configBuilder.build());

      awaitUntilConnectorIsReady();

      // Insert a record and update it.
      TestHelper.execute(String.format(formatInsertString, 1));
      TestHelper.execute("UPDATE t1 SET last_name='some_last_name' where id = 1;");
      TestHelper.execute("UPDATE t1 SET first_name='V', last_name='K', hours=0.05 where id = 1;");

      // Consume the records and verify that the records should have the relevant information.
      List<SourceRecord> records = new ArrayList<>();
      CompletableFuture.runAsync(() -> getRecords(records, 3, 20000)).get();

      // The first record is an insert record with before image as null.
      SourceRecord insertRecord = records.get(0);
      assertValueField(insertRecord, "before", null);
      assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

      // The second record will be an update record having a before image.
      SourceRecord updateRecord = records.get(1);
      assertBeforeImage(updateRecord, 1, "Vaibhav", "Kushwaha", 12.345);
      assertAfterImage(updateRecord, 1, "Vaibhav", "some_last_name", 12.345);

      // The third record will be an update record too.
      SourceRecord updateRecord2 = records.get(2);
      assertBeforeImage(updateRecord2, 1, "Vaibhav", "some_last_name", 12.345);
      assertAfterImage(updateRecord2, 1, "V", "K", 0.05);
  }

  @Test
  public void multiShardTransactions() throws Exception {
    TestHelper.initDB("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", true /* withBeforeImage */);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    start(YugabyteDBConnector.class, configBuilder.build());

    awaitUntilConnectorIsReady();

    // Perform operations on the table
    TestHelper.execute("BEGIN; " + String.format(formatInsertString, 1) + " COMMIT;");
    TestHelper.execute("BEGIN; " + String.format(formatInsertString, 2) + " COMMIT;");
    TestHelper.execute("BEGIN; UPDATE t1 SET hours=98.765 where id = 1; COMMIT;");
    TestHelper.execute("BEGIN; UPDATE t1 SET first_name='first_name_12345' where id = 2; COMMIT;");
    TestHelper.execute("DELETE from t1 WHERE id = 1;");
    TestHelper.execute("DELETE from t1 WHERE id = 2;");

    // The above statements will generate 8 records:
    // INSERT + INSERT + UPDATE + UPDATE + DELETE + TOMBSTONE + DELETE + TOMBSTONE.
    int totalRecordsToConsume = 8;

    // Consume the records and verify that the records should have the relevant information.
    List<SourceRecord> records = new ArrayList<>();
    CompletableFuture.runAsync(() -> getRecords(records, totalRecordsToConsume, 20000)).get();

    // The first and second records will be insert records with before image as null.
    SourceRecord record0 = records.get(0);
    assertValueField(record0, "before", null);
    assertAfterImage(record0, 1, "Vaibhav", "Kushwaha", 12.345);

    SourceRecord record1 = records.get(1);
    assertValueField(record1, "before", null);
    assertAfterImage(record1, 2, "Vaibhav", "Kushwaha", 12.345);

    // The third and fourth records will be update records.
    SourceRecord record2 = records.get(2);
    assertBeforeImage(record2, 1, "Vaibhav", "Kushwaha", 12.345);
    assertAfterImage(record2, 1, "Vaibhav", "Kushwaha", 98.765);

    SourceRecord record3 = records.get(3);
    assertBeforeImage(record3, 2, "Vaibhav", "Kushwaha", 12.345);
    assertAfterImage(record3, 2, "first_name_12345", "Kushwaha", 12.345);

    // For deletes, we will be getting 4 records i.e. DELETE+TOMBSTONE+DELETE+TOMBSTONE.
    SourceRecord record4 = records.get(4);
    assertBeforeImage(record4, 1, "Vaibhav", "Kushwaha", 98.765);
    assertValueField(record4, "after", null); // Delete records have null after field.

    assertTombstone(records.get(5));

    SourceRecord record6 = records.get(6);
    assertBeforeImage(record6, 2, "first_name_12345", "Kushwaha", 12.345);
    assertValueField(record6, "after", null); // Delete records have null after field.

    assertTombstone(records.get(7));
  }

  @Test
  public void updateWithNullValues() throws Exception {
    TestHelper.initDB("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", true /* withBeforeImage */);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    start(YugabyteDBConnector.class, configBuilder.build());

    awaitUntilConnectorIsReady();

    // Insert a record and update it.
    TestHelper.execute(String.format(formatInsertString, 1));
    TestHelper.execute("UPDATE t1 SET last_name=null, hours=null where id = 1;");

    // Consume the records and verify that the records should have the relevant information.
    List<SourceRecord> records = new ArrayList<>();
    CompletableFuture.runAsync(() -> getRecords(records, 2, 20000)).get();

    // The first record is an insert record with before image as null.
    SourceRecord insertRecord = records.get(0);
    assertValueField(insertRecord, "before", null);
    assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

    // The second record will be an update record having a before image.
    SourceRecord updateRecord = records.get(1);
    assertBeforeImage(updateRecord, 1, "Vaibhav", "Kushwaha", 12.345);
    assertAfterImage(updateRecord, 1, "Vaibhav", null, null);
  }

  @Test
  public void modifyPrimaryKey() throws Exception {
    // NOTE: The modification of primary key will not lead to any different behaviour, it will
    // simply give us two records, DELETE + INSERT.

    TestHelper.initDB("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", true /* withBeforeImage */);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    start(YugabyteDBConnector.class, configBuilder.build());

    awaitUntilConnectorIsReady();

    // Insert a record and update it.
    TestHelper.execute(String.format(formatInsertString, 1));
    TestHelper.execute("UPDATE t1 SET last_name='some_last_name', hours=98.765 where id = 1;");
    TestHelper.execute("UPDATE t1 SET id = 404 WHERE id = 1;");

    // Consume the records and verify that the records should have the relevant information.
    List<SourceRecord> records = new ArrayList<>();
    CompletableFuture.runAsync(() -> getRecords(records, 5, 20000)).get();

    // The first record is an insert record with before image as null.
    SourceRecord record0 = records.get(0);
    assertValueField(record0, "before", null);
    assertAfterImage(record0, 1, "Vaibhav", "Kushwaha", 12.345);

    // The second record will be an update record having a before image.
    SourceRecord record1 = records.get(1);
    assertBeforeImage(record1, 1, "Vaibhav", "Kushwaha", 12.345);
    assertAfterImage(record1, 1, "Vaibhav", "some_last_name", 98.765);

    // For updating the primary key, we will get a delete record along with a tombstone and one
    // insert record.
    SourceRecord record2 = records.get(2);
    assertBeforeImage(record2, 1, "Vaibhav", "some_last_name", 98.765);
    assertValueField(record2, "after", null);

    assertTombstone(records.get(3));

    SourceRecord record4 = records.get(4);
    assertValueField(record4, "before", null);
    assertAfterImage(record4, 404, "Vaibhav", "some_last_name", 98.765);
  }

  @Test
  public void operationsOnTableWithDefaultValues() throws Exception {
    TestHelper.initDB("yugabyte_create_tables.ddl");

    // Create a table with default values.
    TestHelper.execute("CREATE TABLE table_with_defaults (id INT PRIMARY KEY, first_name TEXT"
                       + " DEFAULT 'first_name_d', last_name VARCHAR(40) DEFAULT 'last_name_d',"
                       + " hours DOUBLE PRECISION DEFAULT 12.345);");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "table_with_defaults",
                                                    true /* withBeforeImage */);
    Configuration.Builder configBuilder =
        TestHelper.getConfigBuilder("public.table_with_defaults", dbStreamId);
    start(YugabyteDBConnector.class, configBuilder.build());

    awaitUntilConnectorIsReady();

    TestHelper.execute("INSERT INTO table_with_defaults VALUES (1);");
    TestHelper.execute(
        "UPDATE table_with_defaults SET first_name='updated_first_name' WHERE id = 1");
    TestHelper.execute("UPDATE table_with_defaults SET first_name='updated_first_name_2',"
                       + " last_name='updated_last_name', hours=98.765 WHERE id = 1");

    // Consume the records and verify that the records should have the relevant information.
    List<SourceRecord> records = new ArrayList<>();
    CompletableFuture.runAsync(() -> getRecords(records, 3, 20000)).get();

    // The first record is an insert record with before image as null.
    SourceRecord record0 = records.get(0);
    assertValueField(record0, "before", null);
    assertAfterImage(record0, 1, "first_name_d", "last_name_d", 12.345);

    // The second and third records will be update records having a before image.
    SourceRecord record1 = records.get(1);
    assertBeforeImage(record1, 1, "first_name_d", "last_name_d", 12.345);
    assertAfterImage(record1, 1, "updated_first_name", "last_name_d", 12.345);

    SourceRecord record2 = records.get(2);
    assertBeforeImage(record2, 1, "updated_first_name", "last_name_d", 12.345);
    assertAfterImage(record2, 1, "updated_first_name_2", "updated_last_name", 98.765);
  }

  @Test
  public void shouldHandleDeletesGracefullyWithoutErrors() throws Exception {
    // TestHelper.initDB("yugabyte_create_tables.ddl");
    int recordsCount = 20000;
    int batchSize = 1000;
    int iterations = recordsCount / batchSize;
    int columnCount = 99;
    long millisecondsToWait = 600 * 1000; // Wait this much for the records to be consumed

    TestHelper.execute("DROP TABLE IF EXISTS t1;");

    String generatedColumns = "";
    
    for (int i = 1; i <= columnCount; ++i) {
      generatedColumns += "v" + i
                          + " varchar(400) default "
                          + "'123456789012345678901234567890123456789012345678901234567890"
                          + "123456789012345678901234567890123456789012345678901234567890"
                          + "123456789012345678901234567890123456789012345678901234567890"
                          + "123456789012345678901234567890123456789012345678901234567890"
                          + "1234567890123456789012345678901234567890123456789012345', ";
    }

    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT, "
                       + generatedColumns
                       + "v100 varchar(400) default '1234567890123456789012345678901234567890"
                       + "1234567890123456789012345678901234567890123456789012345')"
                       + " SPLIT INTO 1 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", true);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    configBuilder.with(YugabyteDBConnectorConfig.TOMBSTONES_ON_DELETE, false);

    YBClient ybClient = TestHelper.getYbClient("127.0.0.1:7100");
    String tableId = TestHelper.getYbTable(ybClient, "t1").getTableId();
    Compactor compactor = new Compactor(tableId);

    start(YugabyteDBConnector.class, configBuilder.build());

    Thread t = new Thread(compactor);
    t.start();
    
    String insertFormat = "INSERT INTO t1(id, name) VALUES (%s, 'value for split table');";

    YugabyteDBConnection ybConnection = TestHelper.create();
    Connection conn = ybConnection.connection();
    Statement st = conn.createStatement();
    
    int beginKey = 0;
    int endKey = beginKey + batchSize;
    for (int i = 0; i < iterations; ++i) {
      LOGGER.info("Inserting generate_series({}, {})", beginKey, endKey - 1);
      st.execute(String.format(insertFormat, "generate_series(" + beginKey + ", " + (endKey-1) + ")"));
      beginKey = endKey;
      endKey = beginKey + batchSize;
    }

    LOGGER.info("Executing update");
    st.execute("UPDATE t1 SET v100='updated value of v100' WHERE id % 2 = 0;");

    LOGGER.info("Executing delete");
    st.execute("DELETE FROM t1 WHERE id % 2 = 0;");

    TestHelper.waitFor(Duration.ofSeconds(15));

    // Consume the records and verify that the records should have the relevant information.
    List<SourceRecord> records = new ArrayList<>();
    long startMillis = System.currentTimeMillis();
    CompletableFuture.runAsync(() -> getRecords(records, recordsCount + recordsCount /* updates + deletes */, millisecondsToWait)).get();
    LOGGER.info("time taken to consume {} records: {}", recordsCount + recordsCount, System.currentTimeMillis() - startMillis);
    
    int updates = 0;
    int deletes = 0;
    // Check for updates
    for (int j = 0; j < records.size(); ++j) {
        LOGGER.debug("Verifying record: {}", j);
        SourceRecord record = records.get(j);
        Struct value = (Struct) record.value();
        String op = value.getString("op");
        if (op.equals("u")) {
            LOGGER.info("ID: {}, Op: u", value.getStruct("after").getStruct("id").getInt32("value"));
            ++updates;
            // before image will have data
            // Assert the columns for before image
            for (int i = 1; i <= columnCount; ++i) {
                assertValueField(record, "before/v" + i + "/value",
                "123456789012345678901234567890123456789012345678901234567890"
                + "123456789012345678901234567890123456789012345678901234567890"
                + "123456789012345678901234567890123456789012345678901234567890"
                + "123456789012345678901234567890123456789012345678901234567890"
                + "1234567890123456789012345678901234567890123456789012345");

                assertValueField(record, "after/v" + i + "/value",
                "123456789012345678901234567890123456789012345678901234567890"
                + "123456789012345678901234567890123456789012345678901234567890"
                + "123456789012345678901234567890123456789012345678901234567890"
                + "123456789012345678901234567890123456789012345678901234567890"
                + "1234567890123456789012345678901234567890123456789012345");
            }
            assertValueField(record, "before/v100/value", "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345");
            
            assertValueField(record, "after/v100/value", "updated value of v100");
        } else if (op.equals("d")) {
            LOGGER.info("ID: {}, Op: d", value.getStruct("before").getStruct("id").getInt32("value"));
            ++deletes;
            assertValueField(record, "after", null);
            
            // before image will have some data
            // Assert the columns for before image
            for (int i = 1; i <= columnCount; ++i) {
                assertValueField(record, "before/v" + i + "/value",
                "123456789012345678901234567890123456789012345678901234567890"
                + "123456789012345678901234567890123456789012345678901234567890"
                + "123456789012345678901234567890123456789012345678901234567890"
                + "123456789012345678901234567890123456789012345678901234567890"
                + "1234567890123456789012345678901234567890123456789012345");
            }

            assertValueField(record, "before/v100/value", "updated value of v100");
        }
    }

    LOGGER.info("Total updates: {} and total deletes: {}", updates, deletes);

    compactor.kill();
  }

  private class Compactor implements Runnable {
    private String tableId;
    private volatile boolean running = true;

    public Compactor(String tableId) {
        this.tableId = tableId;
    }

    @Override
    public void run() {
      LOGGER.info("Starting compactor thread");
      try {
        // Run compact table
        YBClient ybClient = TestHelper.getYbClient("127.0.0.1:7100");

        while (true) {
            if (!running) {
                break;
            }

            LOGGER.info("Compacting the table");
            ybClient.flushTable(this.tableId);

            Awaitility.await().pollDelay(Duration.ofSeconds(10)).atMost(Duration.ofSeconds(11))
                .until(() -> {return true;});
        }
      } catch (Exception e) {
        LOGGER.error("Exception while getting client or flushing table", e);
        LOGGER.error("Shutting down compactor thread");
      }
    }

    public void kill() {
        running = false;
    }
  }

  private void assertBeforeImage(SourceRecord record, Integer id, String firstName, String lastName,
                                 Double hours) {
      assertValueField(record, "before/id/value", id);
      assertValueField(record, "before/first_name/value", firstName);
      assertValueField(record, "before/last_name/value", lastName);
      assertValueField(record, "before/hours/value", hours);
  }

  private void assertAfterImage(SourceRecord record, Integer id, String firstName, String lastName,
                                Double hours) {
      assertValueField(record, "after/id/value", id);
      assertValueField(record, "after/first_name/value", firstName);
      assertValueField(record, "after/last_name/value", lastName);
      assertValueField(record, "after/hours/value", hours);
  }

  private void getRecords(List<SourceRecord> records, long totalRecordsToConsume,
                          long milliSecondsToWait) {
      AtomicLong totalConsumedRecords = new AtomicLong();
      long seconds = milliSecondsToWait / 1000;
      try {
          Awaitility.await()
              .atMost(Duration.ofSeconds(seconds))
              .until(() -> {
                  int consumed = super.consumeAvailableRecords(record -> {
                      LOGGER.debug("The record being consumed is " + record);
                      records.add(record);
                  });
                  if (consumed > 0) {
                      totalConsumedRecords.addAndGet(consumed);
                      LOGGER.debug("Consumed " + totalConsumedRecords + " records");
                  }

                  return totalConsumedRecords.get() == totalRecordsToConsume;
              });
      } catch (ConditionTimeoutException exception) {
          fail("Failed to consume " + totalRecordsToConsume + " records in " + seconds + " seconds, total: " + totalConsumedRecords.get(),
               exception);
      }

      assertEquals(totalRecordsToConsume, totalConsumedRecords.get());
  }
}
