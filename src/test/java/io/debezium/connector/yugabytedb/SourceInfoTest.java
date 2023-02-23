package io.debezium.connector.yugabytedb;

import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.data.VerifyRecord;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class SourceInfoTest extends YugabyteDBTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceInfoTest.class);
    private SourceInfo source;

    private final String DUMMY_TABLE_NAME = "dummy_table";
    private final String DUMMY_TABLET_ID = "tabletId";

    @BeforeEach
    public void beforeEach() {
        try {
            source = new SourceInfo(new YugabyteDBConnectorConfig(
                    TestHelper.getConfigBuilder("public." + DUMMY_TABLE_NAME, "dummyStreamId").build()
            ));
        } catch (Exception e) {
            LOGGER.error("Exception while initializing the configuration builder", e);
            fail();
        }

        source.update(DUMMY_TABLET_ID, OpId.valueOf("::::"), 123L, "txId",
                      new TableId("yugabyte", "public", DUMMY_TABLE_NAME), 123L, 123L);
    }

    @Test
    public void versionPresent() {
        assertEquals(Module.version(), source.struct().getString(SourceInfo.DEBEZIUM_VERSION_KEY));
    }

    @Test
    public void shouldHaveTableSchema() {
        assertEquals("public", source.struct().getString(SourceInfo.SCHEMA_NAME_KEY));
    }

    @Test
    public void shouldHaveTableName() {
        assertEquals(DUMMY_TABLE_NAME, source.struct().getString(SourceInfo.TABLE_NAME_KEY));
    }

    @Test
    public void shouldHaveCommitTime() {
        assertEquals(123L, source.struct().getInt64(SourceInfo.COMMIT_TIME));
    }

    @Test
    public void shouldHaveRecordTime() {
        assertEquals(123L, source.struct().getInt64(SourceInfo.RECORD_TIME));
    }

    @Test
    public void shouldHaveLsn() {
        assertEquals("::::", source.struct().getString(SourceInfo.LSN_KEY));
    }

    @Test
    public void isSchemaCorrect() {
        final Schema schema = SchemaBuilder.struct()
                .name("io.debezium.connector.yugabytedb.Source")
                .field(SourceInfo.DEBEZIUM_VERSION_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.DEBEZIUM_CONNECTOR_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.SERVER_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .field(SourceInfo.SNAPSHOT_KEY, AbstractSourceInfoStructMaker.SNAPSHOT_RECORD_SCHEMA)
                .field(SourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.SEQUENCE_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TXID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.XMIN_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.TABLET_ID, Schema.STRING_SCHEMA)
                .field(SourceInfo.COMMIT_TIME, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.RECORD_TIME, Schema.INT64_SCHEMA)
                .build();

        VerifyRecord.assertConnectSchemasAreEqual(null, source.struct().schema(), schema);
    }
}
