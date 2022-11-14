package io.debezium.connector.yugabytedb;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Struct;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.YugabyteYSQLContainer;
import org.yb.cdc.CdcService.TabletCheckpointPair;
import org.yb.client.GetTabletListToPollForCDCResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.data.VerifyRecord;
import io.debezium.util.Strings;

/**
 * Unit tests to verify that the connector gracefully handles the tablet splitting on the server.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBTabletSplitTest extends YugabyteDBTestBase {
  private final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBPartitionTest.class);
  private static YugabyteYSQLContainer ybContainer;
  private static String masterAddresses;

  @BeforeClass
  public static void beforeClass() throws SQLException {
      // ybContainer = TestHelper.getYbContainer();
      // ybContainer.start();

      // TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433));
      // TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
      // masterAddresses = ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100);
      masterAddresses = "127.0.0.1:7100";

      TestHelper.dropAllSchemas();
  }

  @Before
  public void before() {
      initializeConnectorTestFramework();
  }

  @After
  public void after() throws Exception {
      stopConnector();
      // TestHelper.executeDDL("drop_tables_and_databases.ddl");
  }

  @AfterClass
  public static void afterClass() throws Exception {
      // ybContainer.stop();
  }

  @Test
  public void shouldConsumeDataAfterTabletSplit() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 1 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    int recordsCount = 50;

    String insertFormat = "INSERT INTO t1 VALUES (%d, 'value for split table');";

    for (int i = 0; i < recordsCount; ++i) {
      TestHelper.execute(String.format(insertFormat, i));
    }

    YBClient ybClient = TestHelper.getYbClient(masterAddresses);
    YBTable table = TestHelper.getYbTable(ybClient, "t1");
    
    // Verify that there is just a single tablet.
    Set<String> tablets = ybClient.getTabletUUIDs(table);
    int tabletCountBeforeSplit = tablets.size();
    assertEquals(1, tabletCountBeforeSplit);

    // Also verify that the new API to get the tablets is returning the correct tablets.
    GetTabletListToPollForCDCResponse getTabletsResponse =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());
    assertEquals(tabletCountBeforeSplit, getTabletsResponse.getTabletCheckpointPairListSize());

    // Compact the table to ready it for splitting.
    ybClient.flushTable(table.getTableId());

    // Wait for 20s for the table to be flushed.
    TestHelper.waitFor(Duration.ofSeconds(20));

    // Split the tablet. There is just one tablet so it is safe to assume that the iterator will
    // return just the desired tablet.
    ybClient.splitTablet(tablets.iterator().next());

    // Wait till there are 2 tablets for the table.
    waitForTablets(ybClient, table, 2);

    // Insert more records
    for (int i = recordsCount; i < 100; ++i) {
      TestHelper.execute(String.format(insertFormat, i));
    }

    // Consume the records now - there will be 100 records in total.
    SourceRecords records = consumeRecordsByTopic(100);
    
    // Verify that the records are there in the topic.
    assertEquals(100, records.recordsForTopic("test_server.public.t1").size());

    // Also call the CDC API to fetch tablets to verify the new tablets have been added in the
    // cdc_state table.
    GetTabletListToPollForCDCResponse getTabletResponse2 =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());

    assertEquals(2, getTabletResponse2.getTabletCheckpointPairListSize());
  }

  @Test
  public void shouldConsumeDataAfterMultipleTabletSplits() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 3 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    int recordsCount = 50;

    String insertFormat = "INSERT INTO t1 VALUES (%d, 'value for split table');";

    for (int i = 0; i < recordsCount; ++i) {
      TestHelper.execute(String.format(insertFormat, i));
    }

    YBClient ybClient = TestHelper.getYbClient(masterAddresses);
    YBTable table = TestHelper.getYbTable(ybClient, "t1");
    
    // Verify that there is just a single tablet.
    Set<String> tablets = ybClient.getTabletUUIDs(table);
    int tabletCountBeforeSplit = tablets.size();
    assertEquals(3, tabletCountBeforeSplit);

    // Also verify that the new API to get the tablets is returning the correct tablets.
    GetTabletListToPollForCDCResponse getTabletsResponse =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());
    assertEquals(tabletCountBeforeSplit, getTabletsResponse.getTabletCheckpointPairListSize());

    // Compact the table to ready it for splitting.
    ybClient.flushTable(table.getTableId());

    // Wait for 20s for the table to be flushed.
    TestHelper.waitFor(Duration.ofSeconds(20));

    // Split 1 tablet.
    String splitTablet1 = tablets.iterator().next();

    ybClient.splitTablet(splitTablet1);

    GetTabletListToPollForCDCResponse r =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId(), splitTablet1);
    
    LOGGER.info("Tablet size in response: {}", r.getTabletCheckpointPairListSize());

    for (TabletCheckpointPair p : r.getTabletCheckpointPairList()) {
      LOGGER.info("Child: {}", p.getTabletId().toStringUtf8());
    }

    // Wait till there are 4 tablets for the table (2 existing plus one tablet split into 2).
    waitForTablets(ybClient, table, 4);

    // Insert more records now --> total 1500 records here.
    TestHelper.executeBulkWithRange(insertFormat, recordsCount, 1500);

    // Wait for the parent tablet to be deleted/hidden before splitting again.
    TestHelper.waitFor(Duration.ofSeconds(20));

    // Flush the table and split all the tablets.
    ybClient.flushTable(table.getTableId());

    // Wait for 20s for the table to be flushed.
    TestHelper.waitFor(Duration.ofSeconds(20));

    // Split each of the new tablets.
    Set<String> tabletSet = ybClient.getTabletUUIDs(table);

    // Verify that there are 4 tablets in total.
    assertEquals(4, tabletSet.size());

    for (String tabletToSplit : tabletSet) {
      ybClient.splitTablet(tabletToSplit);
    }

    // Wait for the tablet count to reach 8 since we would have split all the existing 4 tablets.
    waitForTablets(ybClient, table, 8);

    // Consume the records now - there will be 1500 records in total.
    SourceRecords records = consumeRecordsByTopic(1500);

    // Verify that the records are there in the topic.
    assertEquals(1500, records.recordsForTopic("test_server.public.t1").size());

    // Verify that the API to read cdc_state is returning the correct set of tablets.
    GetTabletListToPollForCDCResponse responseAfterSecondSplit = ybClient.getTabletListToPollForCdc(
        table, dbStreamId, table.getTableId());
    assertEquals(8, responseAfterSecondSplit.getTabletCheckpointPairListSize());
  }

  @Test
  public void startStopConnector() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 3 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    int recordsCount = 100000;

    String insertFormat = "INSERT INTO t1 VALUES (%d, 'value for split table');";

    // for (int i = 0; i < recordsCount; ++i) {
    //   TestHelper.execute(String.format(insertFormat, i));
    // }

    TestHelper.executeBulkWithRange(insertFormat, 0, recordsCount);

    YBClient ybClient = TestHelper.getYbClient(masterAddresses);
    YBTable table = TestHelper.getYbTable(ybClient, "t1");
    
    // Verify that there is just a single tablet.
    Set<String> tablets = ybClient.getTabletUUIDs(table);
    int tabletCountBeforeSplit = tablets.size();
    assertEquals(3, tabletCountBeforeSplit);

    // Also verify that the new API to get the tablets is returning the correct tablets.
    GetTabletListToPollForCDCResponse getTabletsResponse =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());
    assertEquals(tabletCountBeforeSplit, getTabletsResponse.getTabletCheckpointPairListSize());

    // Compact the table to ready it for splitting.
    ybClient.flushTable(table.getTableId());

    // Wait for 20s for the table to be flushed.
    TestHelper.waitFor(Duration.ofSeconds(20));

    // Split 1 tablet.
    ybClient.splitTablet(tablets.iterator().next());

    // Wait till there are 4 tablets for the table (2 existing plus one tablet split into 2).
    waitForTablets(ybClient, table, 4);

    SourceRecords recordsBeforeStoppingConnector = consumeRecordsByTopic(recordsCount);
    assertEquals(recordsCount,
                 recordsBeforeStoppingConnector.recordsForTopic("test_server.public.t1").size());

    // Stop the connector here.
    stopConnector();

    // Insert more records now --> total 1500 records here.
    TestHelper.executeBulkWithRange(insertFormat, recordsCount, 15000);

    // Wait for the parent tablet to be deleted/hidden before splitting again.
    TestHelper.waitFor(Duration.ofSeconds(30));

    // Flush the table and split all the tablets.
    ybClient.flushTable(table.getTableId());

    // Wait for 20s for the table to be flushed.
    TestHelper.waitFor(Duration.ofSeconds(20));

    // Split each of the new tablets.
    Set<String> tabletSet = ybClient.getTabletUUIDs(table);

    // Verify that there are 4 tablets in total.
    assertEquals(4, tabletSet.size());

    for (String tabletToSplit : tabletSet) {
      ybClient.splitTablet(tabletToSplit);
    }

    // Wait for the tablet count to reach 8 since we would have split all the existing 4 tablets.
    waitForTablets(ybClient, table, 8);

    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    // Consume the records now - there will be 1450 records in total.
    SourceRecords records = consumeRecordsByTopic(10000);

    // Verify that the records are there in the topic.
    assertEquals(10000, records.recordsForTopic("test_server.public.t1").size());

    // Verify that the API to read cdc_state is returning the correct set of tablets.
    GetTabletListToPollForCDCResponse responseAfterSecondSplit = ybClient.getTabletListToPollForCdc(
        table, dbStreamId, table.getTableId());
    assertEquals(8, responseAfterSecondSplit.getTabletCheckpointPairListSize());
  }

  @Test
  public void reproduceLocalTests() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 3 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    int recordsCount = 100000;

    String insertFormat = "INSERT INTO t1 VALUES (%d, 'value for split table');";

    TestHelper.executeBulkWithRange(insertFormat, 0, recordsCount);

    YBClient ybClient = TestHelper.getYbClient(masterAddresses);
    YBTable table = TestHelper.getYbTable(ybClient, "t1");
    
    // Verify that there is just a single tablet.
    Set<String> tablets = ybClient.getTabletUUIDs(table);
    int tabletCountBeforeSplit = tablets.size();
    assertEquals(3, tabletCountBeforeSplit);

    // Also verify that the new API to get the tablets is returning the correct tablets.
    GetTabletListToPollForCDCResponse getTabletsResponse =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());
    assertEquals(tabletCountBeforeSplit, getTabletsResponse.getTabletCheckpointPairListSize());

    Awaitility.await()
      .pollDelay(Duration.ofSeconds(2))
      .atMost(Duration.ofSeconds(20))
      .until(() -> {
        return ybClient.getTabletUUIDs(table).size() >= 4;
      });

    SourceRecords recordsBeforeStoppingConnector = consumeRecordsByTopic(recordsCount);
    assertEquals(recordsCount,
                 recordsBeforeStoppingConnector.recordsForTopic("test_server.public.t1").size());

    // Stop the connector here.
    stopConnector();
  }

  @Test
  public void splitWhileTransactionIsNotFinished() throws Exception {
    TestHelper.dropAllSchemas();
    String generatedColumns = "";
    for (int i = 1; i <= 99; ++i) {
      generatedColumns += "v" + i + " varchar(400) default '1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', ";
    }
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT, " + generatedColumns + "v100 varchar(400) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345') SPLIT INTO 1 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    int recordsCount = 20000;

    String insertFormat = "INSERT INTO t1(id, name) VALUES (%d, 'value for split table');";

    YBClient ybClient = TestHelper.getYbClient(masterAddresses);
    YBTable table = TestHelper.getYbTable(ybClient, "t1");

    int beginKey = 0;
    int endKey = beginKey + 100;
    for (int i = 0; i < 200; ++i) {
      TestHelper.executeBulkWithRange(insertFormat, beginKey, endKey);
      beginKey = endKey;
      endKey = beginKey + 100;
      LOGGER.info("At the end of iteration {}, tablet count: {}", i, ybClient.getTabletUUIDs(table).size());
      // verifyRecordCount(200);
    }

    // TestHelper.executeBulkWithRange(insertFormat, 0, recordsCount);
    // for (int i = 0; i < recordsCount; ++i) {
    //   TestHelper.execute(String.format(insertFormat, i));
    // }
    
    // Verify that there is just a single tablet.
    // Set<String> tablets = ybClient.getTabletUUIDs(table);
    // int tabletCountBeforeSplit = tablets.size();
    // assertEquals(1, tabletCountBeforeSplit);

    // Wait for splitting here
    TestHelper.waitFor(Duration.ofSeconds(15));

    LOGGER.info("Tablets after inserting and waiting {} records: {}", recordsCount, ybClient.getTabletUUIDs(table).size());

    // Also verify that the new API to get the tablets is returning the correct tablets.
    // GetTabletListToPollForCDCResponse getTabletsResponse =
    //   ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());
    // assertEquals(tabletCountBeforeSplit, getTabletsResponse.getTabletCheckpointPairListSize());

    // Start a transaction
    // YugabyteDBConnection ybConn = TestHelper.create();
    // Statement st = ybConn.connection().createStatement();
    // st.execute("BEGIN;");

    // TestHelper.executeBulkWithRange(insertFormat, recordsCount /* 50000 */, 10000);

    // Compact the table to ready it for splitting.
    // ybClient.flushTable(table.getTableId());

    // Wait for 20s for the table to be flushed.
    // TestHelper.waitFor(Duration.ofSeconds(20));

    // Split the tablet. There is just one tablet so it is safe to assume that the iterator will
    // return just the desired tablet.
    // ybClient.splitTablet(tablets.iterator().next());

    // Insert more records
    // st.execute("INSERT INTO t1(id, name) VALUES (generate_series(10000, 24999), 'value for split table');");
    // for (int i = 10000; i < 25000; ++i) {
    //   st.execute("BEGIN; " + String.format(insertFormat, i) + " COMMIT;");
    // }

    // st.execute("COMMIT;");

    // Wait till there are 2 tablets for the table.
    // waitForTablets(ybClient, table, 2);
    // for (int i = 0; i < 15; ++i) {
    //   LOGGER.info("VKVK tablet count: {}", ybClient.getTabletUUIDs(table).size());
    // }

    // Consume the records now - there will be 100 records in total.

    CompletableFuture.runAsync(() -> verifyRecordCount(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();

    LOGGER.info("Tablets after consuming records: {}", ybClient.getTabletUUIDs(table).size());
    // SourceRecords records = consumeRecordsByTopic(recordsCount);
    
    // Verify that the records are there in the topic.
    // assertEquals(recordsCount, records.recordsForTopic("test_server.public.t1").size());

    // Also call the CDC API to fetch tablets to verify the new tablets have been added in the
    // cdc_state table.
    // GetTabletListToPollForCDCResponse getTabletResponse2 =
    //   ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());

    // assertTrue(getTabletResponse2.getTabletCheckpointPairListSize() >= 2);
    
  }

  @Test
  public void splitWithSingleShard() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT, v1 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v2 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', "
                     + "v3 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v4 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v5 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v6 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v7 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345',"
                     + "v8 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v9 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v10 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v11 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v12 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', "
                     + "v13 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v14 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v15 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v16 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v17 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', "
                     + "v18 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v19 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v20 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v21 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v22 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', "
                     + "v23 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v24 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v25 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v26 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v27 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', "
                     + "v28 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v29 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', v30 varchar(100) default '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345') SPLIT INTO 1 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    int recordsCount = 1000000;

    String insertFormat = "INSERT INTO t1(id, name) VALUES (%d, 'value for split table');";

    TestHelper.executeBulkWithRange(insertFormat, 0, recordsCount);
    // for (int i = 0; i < recordsCount; ++i) {
    //   TestHelper.execute(String.format(insertFormat, i));
    // }

    YBClient ybClient = TestHelper.getYbClient(masterAddresses);
    YBTable table = TestHelper.getYbTable(ybClient, "t1");
    
    // Verify that there is just a single tablet.
    Set<String> tablets = ybClient.getTabletUUIDs(table);
    int tabletCountBeforeSplit = tablets.size();
    assertEquals(1, tabletCountBeforeSplit);

    // Also verify that the new API to get the tablets is returning the correct tablets.
    GetTabletListToPollForCDCResponse getTabletsResponse =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());
    assertEquals(tabletCountBeforeSplit, getTabletsResponse.getTabletCheckpointPairListSize());

    // Start a transaction
    YugabyteDBConnection ybConn = TestHelper.create();
    Statement st = ybConn.connection().createStatement();
    // st.execute("BEGIN;");

    TestHelper.executeBulkWithRange(insertFormat, recordsCount /* 100000 */, 200000);

    // Insert more records
    for (int i = 200000; i < 350000; ++i) {
      st.execute(String.format(insertFormat, i));
    }
    // st.execute("INSERT INTO t1(id, name) VALUES (generate_series(200000, 349999), 'value for split table');");

    for (int i = 0; i < 15; ++i) {
      LOGGER.info("VKVK tablet count: {}", ybClient.getTabletUUIDs(table).size());
    }

    // Consume the records now - there will be 100 records in total.
    SourceRecords records = consumeRecordsByTopic(350000);
    
    // Verify that the records are there in the topic.
    assertEquals(350000, records.recordsForTopic("test_server.public.t1").size());

    // Also call the CDC API to fetch tablets to verify the new tablets have been added in the
    // cdc_state table.
    GetTabletListToPollForCDCResponse getTabletResponse2 =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());

    assertTrue(getTabletResponse2.getTabletCheckpointPairListSize() >= 2);
    // assertEquals(2, );
  }

  @Test
  public void printApiTimings() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 1 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    YBClient ybClient = TestHelper.getYbClient(masterAddresses);
    YBTable table = TestHelper.getYbTable(ybClient, "t1");

  }

  private void verifyRecordCount(long recordsCount) {
    int totalConsumedRecords = 0;
    long start = System.currentTimeMillis();
    Set<Integer> recordKeySet = new HashSet<>();
    int failureCounter = 0;
    while (recordKeySet.size() < recordsCount) {
        int consumed = super.consumeAvailableRecords(record -> {
            LOGGER.debug("The record being consumed is " + record + " with offset: " + record.sourceOffset() + " and partition: " + record.sourcePartition());
            Struct s = (Struct) record.key();
            int value = s.getStruct("id").getInt32("value");
            LOGGER.debug("VKVK key value: " + value);
            recordKeySet.add(value);
        });
        if (consumed > 0) {
            failureCounter = 0;
            totalConsumedRecords += consumed;
            LOGGER.info("Consumed " + totalConsumedRecords + " records with record key set size: " + recordKeySet.size());
        } else {
          ++failureCounter;
          TestHelper.waitFor(Duration.ofSeconds(2));
        }

        if (failureCounter == 100) {
          LOGGER.info("Breaking becauase failure counter hit limit");
          break;
        }
    }
    LOGGER.info("Total duration to consume " + recordsCount + " records: " + Strings.duration(System.currentTimeMillis() - start));

    LOGGER.info("VKVK record key set size: " + recordKeySet.size());
    List<Integer> rList = recordKeySet.stream().collect(Collectors.toList());
    Collections.sort(rList);
    LOGGER.info("VKVK record key set: " + rList.toString());
    
    assertEquals(recordsCount, recordKeySet.size());
}

  private void waitForTablets(YBClient ybClient, YBTable table, int tabletCount) {
    Awaitility.await()
      .pollDelay(Duration.ofSeconds(2))
      .atMost(Duration.ofSeconds(20))
      .until(() -> {
        return ybClient.getTabletUUIDs(table).size() == tabletCount;
      });
  }
}
