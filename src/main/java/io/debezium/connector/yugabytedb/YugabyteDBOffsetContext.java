/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.spi.OffsetState;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;

public class YugabyteDBOffsetContext implements OffsetContext {
    public static final String LAST_COMPLETELY_PROCESSED_LSN_KEY = "lsn_proc";
    public static final String SNAPSHOT_DONE_KEY = "snapshot_done_key";

    private static final Logger LOGGER = LoggerFactory
            .getLogger(YugabyteDBSnapshotChangeEventSource.class);
    private final Schema sourceInfoSchema;
    private final Map<String, SourceInfo> tabletSourceInfo;
    private final Map<String, Boolean> tableColocationInfo;
    private final SourceInfo sourceInfo;
    private boolean lastSnapshotRecord;
    private OpId lastCompletelyProcessedLsn;
    private OpId lastCommitLsn;
    private OpId streamingStoppingLsn = null;
    private TransactionContext transactionContext;
    private IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
    private YugabyteDBConnectorConfig connectorConfig;

    private YugabyteDBOffsetContext(YugabyteDBConnectorConfig connectorConfig,
                                    OpId lsn, OpId lastCompletelyProcessedLsn,
                                    OpId lastCommitLsn,
                                    String txId,
                                    Instant time,
                                    boolean snapshot,
                                    boolean lastSnapshotRecord,
                                    TransactionContext transactionContext,
                                    IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        sourceInfo = new SourceInfo(connectorConfig);
        this.tabletSourceInfo = new ConcurrentHashMap();
        this.tableColocationInfo = new ConcurrentHashMap<>();
        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
        this.lastCommitLsn = lastCommitLsn;
        // sourceInfo.update(lsn, time, txId, null, sourceInfo.xmin());
        sourceInfo.updateLastCommit(lastCommitLsn);
        sourceInfoSchema = sourceInfo.schema();

        this.lastSnapshotRecord = lastSnapshotRecord;
        if (this.lastSnapshotRecord) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
        this.connectorConfig = connectorConfig;
    }

    public YugabyteDBOffsetContext(Offsets<YBPartition, YugabyteDBOffsetContext> previousOffsets,
                                   YugabyteDBConnectorConfig config) {
        this.tabletSourceInfo = new ConcurrentHashMap();
        this.tableColocationInfo = new ConcurrentHashMap<>();
        this.sourceInfo = new SourceInfo(config);
        this.sourceInfoSchema = sourceInfo.schema();

        for (Map.Entry<YBPartition, YugabyteDBOffsetContext> context :
                previousOffsets.getOffsets().entrySet()) {
            YugabyteDBOffsetContext c = context.getValue();
            if (c != null) {
                this.lastCompletelyProcessedLsn = c.lastCompletelyProcessedLsn;
                this.lastCommitLsn = c.lastCommitLsn;
                String tableUUID = context.getKey().getTableId();
                String tabletId = context.getKey().getTabletId();
                initSourceInfo(tableUUID, tabletId, config);
                this.updateWalPosition(tableUUID, tabletId,
                        this.lastCommitLsn, lastCompletelyProcessedLsn, null, null, null, null);
            }
        }
        LOGGER.debug("Populating the tabletsourceinfo with " + this.getTabletSourceInfo());
        this.transactionContext = new TransactionContext();
        this.incrementalSnapshotContext = new SignalBasedIncrementalSnapshotContext<>();
        this.connectorConfig = config;
    }

    public static YugabyteDBOffsetContext initialContextForSnapshot(YugabyteDBConnectorConfig connectorConfig,
                                                                    YugabyteDBConnection jdbcConnection,
                                                                    Clock clock,
                                                                    Set<YBPartition> partitions) {
        return initialContext(connectorConfig, jdbcConnection, clock, new OpId(-1, -1, "".getBytes(), -1, 0),
                new OpId(-1, -1, "".getBytes(), -1, 0), partitions);
    }

    public static YugabyteDBOffsetContext initialContext(YugabyteDBConnectorConfig connectorConfig,
                                                         YugabyteDBConnection jdbcConnection,
                                                         Clock clock,
                                                         Set<YBPartition> partitions) {
        return initialContext(connectorConfig, jdbcConnection, clock, new OpId(0, 0, "".getBytes(), 0, 0),
                new OpId(0, 0, "".getBytes(), 0, 0), partitions);
    }

    public static YugabyteDBOffsetContext initialContext(YugabyteDBConnectorConfig connectorConfig,
                                                         YugabyteDBConnection jdbcConnection,
                                                         Clock clock,
                                                         OpId lastCommitLsn,
                                                         OpId lastCompletelyProcessedLsn,
                                                         Set<YBPartition> partitions) {
        LOGGER.info("Creating initial offset context");

        final long txId = 0L;// new OpId(0,0,"".getBytes(), 0);

        YugabyteDBOffsetContext context = new YugabyteDBOffsetContext(
                connectorConfig,
                null, /* passing null since this value is not being used anywhere in constructor */
                lastCompletelyProcessedLsn,
                lastCommitLsn,
                String.valueOf(txId),
                clock.currentTimeAsInstant(),
                false,
                false,
                new TransactionContext(),
                new SignalBasedIncrementalSnapshotContext<>());
        for (YBPartition p : partitions) {
            if (context.getTabletSourceInfo().get(p.getId()) == null) {
                // While initializing partitions, we do pass the information whether a table is
                // colocated, utilize the same information to further initialize context.
                if (p.isTableColocated()) {
                    context.markTableAsColocated(p.getTableId());
                } else {
                    context.markTableNonColocated(p.getTableId());
                }

                context.initSourceInfo(p.getTableId(), p.getTabletId(), connectorConfig);
                context.updateWalPosition(p.getTableId(), p.getTabletId(), lastCommitLsn, lastCompletelyProcessedLsn, clock.currentTimeAsInstant(), String.valueOf(txId), null, null);
            }
        }
        return context;
    }

    /**
     * @return the starting {@link OpId} to begin the snapshot with
     */
    public static OpId snapshotStartLsn() {
        return new OpId(-1, -1, "".getBytes(), -1, 0);
    }

    /**
     * @return the starting {@link OpId} to begin the streaming with
     */
    public static OpId streamingStartLsn() {
        return new OpId(0, 0, "".getBytes(), 0, 0);
    }

    /**
     * @return the {@link OpId} which tells the server that the connector has marked the snapshot
     * as completed, and it is now transitioning towards streaming
     */
    public static OpId snapshotDoneKeyLsn() {
        return new OpId(0, 0, SNAPSHOT_DONE_KEY.getBytes(), 0, 0);
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> result = new HashMap<>();

        if (this.tabletSourceInfo == null) {
            LOGGER.info("tablet source Info is null");
        }

        for (Map.Entry<String, SourceInfo> entry : this.tabletSourceInfo.entrySet()) {
            // The entry.getKey() here would be tableId.tabletId
            if (entry.getKey() == null) {
                LOGGER.info("entry.getKey is null");
            }

            if (entry.getValue() == null) {
                LOGGER.info("entry.getValue() is null for {}", entry.getKey());
            }

            if (entry.getValue().lsn() == null) {
                LOGGER.info("entry.getValue().lsn() is null for {}", entry.getKey());
            }
            result.put(entry.getKey(), entry.getValue().lsn().toSerString());
        }

        return sourceInfo.isSnapshot() ? result
                : incrementalSnapshotContext
                        .store(transactionContext.store(result));
    }

    public Struct getSourceInfoForTablet(String tableId, String tabletId) {
        if (!tableColocationInfo.get(tableId)) {
            return this.tabletSourceInfo.get(tabletId).struct();
        }

        return this.tabletSourceInfo.get(tableId + "." + tabletId).struct();
    }

    public void markTableAsColocated(String tableUUID) {
        this.tableColocationInfo.put(tableUUID, true);
    }

    public void markTableNonColocated(String tableUUID) {
        this.tableColocationInfo.put(tableUUID, false);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    public SourceInfo getSourceInfo(String tableId, String tabletId) {
        if (!tableColocationInfo.get(tableId)) {
            SourceInfo info = tabletSourceInfo.get(tabletId);
            if (info == null) {
                tabletSourceInfo.put(tabletId, new SourceInfo(connectorConfig, YugabyteDBOffsetContext.streamingStartLsn(), false));
            }
            return tabletSourceInfo.get(tabletId);
        }

        return tabletSourceInfo.get(tableId + "." + tabletId);
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot();
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        lastSnapshotRecord = false;
    }

    @Override
    public void preSnapshotCompletion() {
        lastSnapshotRecord = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    public void updateSnapshotPosition(Instant timestamp, TableId tableId) {
        sourceInfo.update(timestamp, tableId);
    }

    public void updateWalPosition(String tableUUID, String tabletId, OpId lsn, OpId lastCompletelyProcessedLsn,
                                  Instant commitTime,
                                  String txId, TableId tableId, Long xmin) {
        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;

        // Only use the tableUUID as the prefix in case the table is colocated.
        String lookupPrefix = tableColocationInfo.get(tableUUID) ? tableUUID + "." : "";

        sourceInfo.update(lookupPrefix, tabletId, lsn, commitTime, txId, tableId, xmin);
        SourceInfo info = this.tabletSourceInfo.get(lookupPrefix + tabletId);

        // There is a possibility upon the transition from snapshot to streaming mode that we try
        // to retrieve a SourceInfo which may not be available in the map as we will just be looking
        // up using the tabletId. Store the SourceInfo in that case.
        if (info == null) {
            info = new SourceInfo(connectorConfig, lsn, tableColocationInfo.get(tableUUID));
        }

        info.update(lookupPrefix, tabletId, lsn, commitTime, txId, tableId, xmin);
        this.tabletSourceInfo.put(lookupPrefix + tabletId, info);
    }

    public void initSourceInfo(String tableUUID, String tabletId, YugabyteDBConnectorConfig connectorConfig) {
        this.tabletSourceInfo.put(tableUUID + "." + tabletId, new SourceInfo(connectorConfig));
    }

    public void initSourceInfo(String tableUUID, String tabletId, YugabyteDBConnectorConfig connectorConfig, OpId opId,
                               boolean colocated) {
        this.tableColocationInfo.put(tableUUID, colocated);
        this.tabletSourceInfo.put((colocated ? tableUUID + "." : "") + tabletId, new SourceInfo(connectorConfig, opId, colocated));
    }

    public Map<String, SourceInfo> getTabletSourceInfo() {
        return tabletSourceInfo;
    }

    public void updateCommitPosition(OpId lsn, OpId lastCompletelyProcessedLsn) {
        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
        this.lastCommitLsn = lsn;
        sourceInfo.updateLastCommit(lsn);
    }

    boolean hasLastKnownPosition() {
        return sourceInfo.lsn() != null;
    }

    boolean hasCompletelyProcessedPosition() {
        return this.lastCompletelyProcessedLsn != null;
    }

    OpId lsn() {
        return sourceInfo.lsn() == null ? streamingStartLsn()
                : sourceInfo.lsn();
    }

    OpId lsn(String tableUUID, String tabletId) {
        // get the sourceInfo of the tablet
        SourceInfo sourceInfo = getSourceInfo(tableUUID, tabletId);
        return sourceInfo.lsn() == null ? streamingStartLsn()
                : sourceInfo.lsn();
    }

    /**
     * If a previous OpId is null then we want the server to send the snapshot from the
     * beginning. Requesting from the term -1, index -1 and empty key would indicate
     * the server that a snapshot needs to be taken and the write ID as -1 tells that we are
     * in the snapshot mode and snapshot time 0 signifies that we are bootstrapping
     * the snapshot flow.
     * <p>
     * In short, we are telling the server to decide an appropriate checkpoint till which the
     * snapshot needs to be taken and send it as a response back to the connector.
     *
     * @param tabletId the tablet UUID
     * @return {@link OpId} from which we need to read the snapshot from the server
     */
    OpId snapshotLSN(String tableUUID, String tabletId) {
      // get the sourceInfo of the tablet
      SourceInfo sourceInfo = getSourceInfo(tableUUID, tabletId);
      return sourceInfo.lsn() == null ? snapshotStartLsn()
        : sourceInfo.lsn();
    }

    OpId lastCompletelyProcessedLsn() {
        return lastCompletelyProcessedLsn;
    }
    
    OpId lastCompletelyProcessedLsn(String tabletId) {
        return lastCompletelyProcessedLsn;
    }

    OpId lastCommitLsn() {
        return lastCommitLsn;
    }

    /**
     * Returns the LSN that the streaming phase should stream events up to or null if
     * a stopping point is not set. If set during the streaming phase, any event with
     * an LSN less than the stopping LSN will be processed and once the stopping LSN
     * is reached, the streaming phase will end. Useful for a pre-snapshot catch up
     * streaming phase.
     */
    OpId getStreamingStoppingLsn() {
        return streamingStoppingLsn;
    }

    public void setStreamingStoppingLsn(OpId streamingStoppingLsn) {
        this.streamingStoppingLsn = streamingStoppingLsn;
    }

    Long xmin() {
        return sourceInfo.xmin();
    }

    @Override
    public String toString() {
        return "YugabyteDBOffsetContext [sourceInfoSchema=" + sourceInfoSchema +
                ", sourceInfo=" + sourceInfo
                + ", lastSnapshotRecord=" + lastSnapshotRecord
                + ", lastCompletelyProcessedLsn=" + lastCompletelyProcessedLsn
                + ", lastCommitLsn=" + lastCommitLsn
                + ", streamingStoppingLsn=" + streamingStoppingLsn
                + ", transactionContext=" + transactionContext
                + ", incrementalSnapshotContext=" + incrementalSnapshotContext
                + ", tabletSourceInfo=" + tabletSourceInfo + "]";
    }

    public OffsetState asOffsetState() {
        return new OffsetState(
                sourceInfo.lsn(),
                sourceInfo.txId(),
                sourceInfo.xmin(),
                sourceInfo.timestamp(),
                sourceInfo.isSnapshot());
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
    }

    @Override
    public void event(DataCollectionId tableId, Instant instant) {
        sourceInfo.update(instant, (TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void incrementalSnapshotEvents() {
        sourceInfo.setSnapshot(SnapshotRecord.INCREMENTAL);
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    public static class Loader implements OffsetContext.Loader<YugabyteDBOffsetContext> {

        private final YugabyteDBConnectorConfig connectorConfig;

        public Loader(YugabyteDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        private Long readOptionalLong(Map<String, ?> offset, String key) {
            final Object obj = offset.get(key);
            return (obj == null) ? null : ((Number) obj).longValue();
        }

        private String readOptionalString(Map<String, ?> offset, String key) {
            final Object obj = offset.get(key);
            return (obj == null) ? null : ((String) obj);
        }

        @SuppressWarnings("unchecked")
        @Override
        public YugabyteDBOffsetContext load(Map<String, ?> offset) {

            LOGGER.debug("The offset being loaded in YugabyteDBOffsetContext.. " + offset);
            OpId lastCompletelyProcessedLsn;
            if (offset != null) {
                lastCompletelyProcessedLsn = OpId.valueOf((String) offset.get(YugabyteDBOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY));
            }
            else {
                lastCompletelyProcessedLsn = new OpId(0, 0, "".getBytes(), 0, 0);
            }
            /*
             * final OpId lsn = OpId.valueOf(readOptionalString(offset, SourceInfo.LSN_KEY));
             * final OpId lastCompletelyProcessedLsn = OpId.valueOf(readOptionalString(offset,
             * LAST_COMPLETELY_PROCESSED_LSN_KEY));
             * final OpId lastCommitLsn = OpId.valueOf(readOptionalString(offset,
             * LAST_COMPLETELY_PROCESSED_LSN_KEY));
             * final String txId = readOptionalString(offset, SourceInfo.TXID_KEY);
             * 
             * final Instant useconds = Conversions.toInstantFromMicros((Long) offset
             * .get(SourceInfo.TIMESTAMP_USEC_KEY));
             * final boolean snapshot = (boolean) ((Map<String, Object>) offset)
             * .getOrDefault(SourceInfo.SNAPSHOT_KEY, Boolean.FALSE);
             * final boolean lastSnapshotRecord = (boolean) ((Map<String, Object>) offset)
             * .getOrDefault(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, Boolean.FALSE);
             * return new YugabyteDBOffsetContext(connectorConfig, lsn, lastCompletelyProcessedLsn,
             * lastCommitLsn, txId, useconds, snapshot, lastSnapshotRecord,
             * TransactionContext.load(offset), SignalBasedIncrementalSnapshotContext
             * .load(offset));
             */

            return new YugabyteDBOffsetContext(connectorConfig,
                    lastCompletelyProcessedLsn,
                    lastCompletelyProcessedLsn,
                    lastCompletelyProcessedLsn,
                    "txId", Instant.MIN, false, false,
                    TransactionContext.load(offset),
                    SignalBasedIncrementalSnapshotContext.load(offset));

        }
    }
}
