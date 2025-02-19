/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import io.debezium.DebeziumException;
import io.debezium.connector.yugabytedb.connection.*;
import io.debezium.connector.yugabytedb.connection.ReplicationMessage.Operation;
import io.debezium.connector.yugabytedb.connection.pgproto.YbProtoReplicationMessage;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.Metronome;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.cdc.CdcService.TabletCheckpointPair;
import org.yb.cdc.CdcService.CDCErrorPB.Code;
import org.yb.client.*;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;

/**
 *
 * @author Suranjan Kumar (skumar@yugabyte.com)
 */
public class YugabyteDBStreamingChangeEventSource implements
        StreamingChangeEventSource<YBPartition, YugabyteDBOffsetContext> {

    private static final String KEEP_ALIVE_THREAD_NAME = "keep-alive";

    /**
     * Number of received events without sending anything to Kafka which will
     * trigger a "WAL backlog growing" warning.
     */
    private static final int GROWING_WAL_WARNING_LOG_INTERVAL = 10_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBStreamingChangeEventSource.class);

    // PGOUTPUT decoder sends the messages with larger time gaps than other decoders
    // We thus try to read the message multiple times before we make poll pause
    private static final int THROTTLE_NO_MESSAGE_BEFORE_PAUSE = 5;

    private final YugabyteDBConnection connection;
    private final YugabyteDBEventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final YugabyteDBSchema schema;
    private final YugabyteDBConnectorConfig connectorConfig;
    private final YugabyteDBTaskContext taskContext;

    private final Snapshotter snapshotter;
    private final DelayStrategy pauseNoMessage;
    private final ElapsedTimeStrategy connectionProbeTimer;

    /**
     * The minimum of (number of event received since the last event sent to Kafka,
     * number of event received since last WAL growing warning issued).
     */
    private long numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
    private OpId lastCompletelyProcessedLsn;

    private final AsyncYBClient asyncYBClient;
    private final YBClient syncClient;
    private YugabyteDBTypeRegistry yugabyteDBTypeRegistry;
    private final Map<String, OpId> checkPointMap;
    private final ChangeEventQueue<DataChangeEvent> queue;

    protected Map<String, CdcSdkCheckpoint> tabletToExplicitCheckpoint;

    public YugabyteDBStreamingChangeEventSource(YugabyteDBConnectorConfig connectorConfig, Snapshotter snapshotter,
                                                YugabyteDBConnection connection, YugabyteDBEventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock,
                                                YugabyteDBSchema schema, YugabyteDBTaskContext taskContext, ReplicationConnection replicationConnection,
                                                ChangeEventQueue<DataChangeEvent> queue) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        pauseNoMessage = DelayStrategy.constant(taskContext.getConfig().getPollInterval().toMillis());
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        checkPointMap = new ConcurrentHashMap<>();
        this.connectionProbeTimer = ElapsedTimeStrategy.constant(Clock.system(), connectorConfig.statusUpdateInterval());

        String masterAddress = connectorConfig.masterAddresses();
        asyncYBClient = new AsyncYBClient.AsyncYBClientBuilder(masterAddress)
                .defaultAdminOperationTimeoutMs(connectorConfig.adminOperationTimeoutMs())
                .defaultOperationTimeoutMs(connectorConfig.operationTimeoutMs())
                .defaultSocketReadTimeoutMs(connectorConfig.socketReadTimeoutMs())
                .numTablets(connectorConfig.maxNumTablets())
                .sslCertFile(connectorConfig.sslRootCert())
                .sslClientCertFiles(connectorConfig.sslClientCert(), connectorConfig.sslClientKey())
                .build();

        syncClient = new YBClient(asyncYBClient);
        yugabyteDBTypeRegistry = taskContext.schema().getTypeRegistry();
        this.queue = queue;
        this.tabletToExplicitCheckpoint = new HashMap<>();
    }

    @Override
    public void execute(ChangeEventSourceContext context, YBPartition partition, YugabyteDBOffsetContext offsetContext) {
        if (!snapshotter.shouldStream()) {
            LOGGER.info("Skipping streaming since it's not enabled in the configuration");
            return;
        }

        Set<YBPartition> partitions = new YBPartition.Provider(connectorConfig).getPartitions();
        boolean hasStartLsnStoredInContext = offsetContext != null && !offsetContext.getTabletSourceInfo().isEmpty();

        LOGGER.info("Starting the change streaming process now");

        if (!hasStartLsnStoredInContext) {
            LOGGER.info("No start opid found in the context.");
                offsetContext = YugabyteDBOffsetContext.initialContext(connectorConfig, connection, clock, partitions);
        }

        try {
            getChanges2(context, partition, offsetContext, hasStartLsnStoredInContext);
        } catch (Throwable e) {
            Objects.requireNonNull(e);
            errorHandler.setProducerThrowable(e);
        }
        finally {

            if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
                // Need to see in CDCSDK what can be done.
            }
            if (asyncYBClient != null) {
              try {
                asyncYBClient.close();
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
            if (syncClient != null) {
              try {
                syncClient.close();
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
        }
    }

    private void bootstrapTablet(YBTable table, String tabletId) throws Exception {
        GetCheckpointResponse getCheckpointResponse = this.syncClient.getCheckpoint(table, connectorConfig.streamId(), tabletId);

        long term = getCheckpointResponse.getTerm();
        long index = getCheckpointResponse.getIndex();
        LOGGER.info("Checkpoint for tablet {} before going to bootstrap: {}.{}", tabletId, term, index);
        if (term == -1 && index == -1) {
            LOGGER.info("Bootstrapping the tablet {}", tabletId);
            this.syncClient.bootstrapTablet(table, connectorConfig.streamId(), tabletId, 0, 0, true, true);
        }
        else {
            LOGGER.info("Skipping bootstrap for table {} tablet {} as it has a checkpoint {}.{}", table.getTableId(), tabletId, term, index);
        }
    }

    private void bootstrapTabletWithRetry(List<Pair<String,String>> tabletPairList) throws Exception {
        short retryCountForBootstrapping = 0;
        for (Pair<String, String> entry : tabletPairList) {
            // entry is a Pair<tableId, tabletId>
            boolean shouldRetry = true;
            while (retryCountForBootstrapping <= connectorConfig.maxConnectorRetries() && shouldRetry) {
                try {
                    bootstrapTablet(this.syncClient.openTableByUUID(entry.getKey()), entry.getValue());

                    // Reset the retry flag if the bootstrap was successful
                    shouldRetry = false;
                } catch (Exception e) {
                    ++retryCountForBootstrapping;

                    // The connector should go for a retry if any exception is thrown
                    shouldRetry = true;

                    if (retryCountForBootstrapping > connectorConfig.maxConnectorRetries()) {
                        LOGGER.error("Failed to bootstrap the tablet {} after {} retries", entry.getValue(), connectorConfig.maxConnectorRetries());
                        throw e;
                    }

                    // If there are retries left, perform them after the specified delay.
                    LOGGER.warn("Error while trying to bootstrap tablet {}; will attempt retry {} of {} after {} milli-seconds. Exception message: {}",
                            entry.getValue(), retryCountForBootstrapping, connectorConfig.maxConnectorRetries(), connectorConfig.connectorRetryDelayMs(), e.getMessage());

                    try {
                        final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
                        retryMetronome.pause();
                    } catch (InterruptedException ie) {
                        LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }


    private void getChanges2(ChangeEventSourceContext context,
                             YBPartition partitionn,
                             YugabyteDBOffsetContext offsetContext,
                             boolean previousOffsetPresent)
            throws Exception {
        LOGGER.info("Processing messages");

        String tabletList = this.connectorConfig.getConfig().getString(YugabyteDBConnectorConfig.TABLET_LIST);

        // This tabletPairList has Pair<String, String> objects wherein the key is the table UUID
        // and the value is tablet UUID
        List<Pair<String, String>> tabletPairList = null;
        try {
            tabletPairList = (List<Pair<String, String>>) ObjectUtil.deserializeObjectFromString(tabletList);
            LOGGER.debug("The tablet list is " + tabletPairList);
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("Exception while deserializing tablet pair list", e);
            throw new RuntimeException(e);
        }

        Map<String, YBTable> tableIdToTable = new HashMap<>();
        Map<String, GetTabletListToPollForCDCResponse> tabletListResponse = new HashMap<>();
        String streamId = connectorConfig.streamId();

        LOGGER.info("Using DB stream ID: " + streamId);

        Set<String> tIds = tabletPairList.stream().map(pair -> pair.getLeft()).collect(Collectors.toSet());
        for (String tId : tIds) {
            LOGGER.debug("Table UUID: " + tIds);
            YBTable table = this.syncClient.openTableByUUID(tId);
            tableIdToTable.put(tId, table);

            GetTabletListToPollForCDCResponse resp =
                this.syncClient.getTabletListToPollForCdc(table, streamId, tId);
            tabletListResponse.put(tId, resp);
        }

        LOGGER.debug("The init tabletSourceInfo before updating is " + offsetContext.getTabletSourceInfo());

        // Initialize the offsetContext and other supporting flags.
        // This schemaNeeded map here would have the elements as <tableId.tabletId>:<boolean-value>
        Map<String, Boolean> schemaNeeded = new HashMap<>();
        for (Pair<String, String> entry : tabletPairList) {
            // entry.getValue() will give the tabletId
            OpId opId = YBClientUtils.getOpIdFromGetTabletListResponse(
                            tabletListResponse.get(entry.getKey()), entry.getValue());

            // If we are getting a term and index as -1 and -1 from the server side it means
            // that the streaming has not yet started on that tablet ID. In that case, assign a
            // starting OpId so that the connector can poll using proper checkpoints.
            if (opId.getTerm() == -1 && opId.getIndex() == -1) {
                opId = YugabyteDBOffsetContext.streamingStartLsn();
            }

            // For streaming, we do not want any colocated information and want to process the tables
            // based on just their tablet IDs - pass false as the 'colocated' flag to enforce the same.
            YBPartition p = new YBPartition(entry.getKey(), entry.getValue(), false /* colocated */);
            offsetContext.initSourceInfo(p, this.connectorConfig, opId);
            schemaNeeded.put(p.getId(), Boolean.TRUE);
        }

        // This will contain the tablet ID mapped to the number of records it has seen
        // in the transactional block. Note that the entry will be created only when
        // a BEGIN block is encountered.
        Map<String, Integer> recordsInTransactionalBlock = new HashMap<>();

        // This will contain the tablet ID mapped to the number of begin records observed for
        // a tablet. Consider the scenario for a colocated tablet with two tables, it is possible
        // that we can encounter BEGIN-BEGIN-COMMIT-COMMIT. To handle this scenario, we need the
        // count for the BEGIN records so that we can verify that we have equal COMMIT records
        // in the stream as well.
        Map<String, Integer> beginCountForTablet = new HashMap<>();

        LOGGER.debug("The init tabletSourceInfo after updating is " + offsetContext.getTabletSourceInfo());

        // Only bootstrap if no snapshot has been enabled - if snapshot is enabled then
        // the assumption is that there will already be some checkpoints for the tablet in
        // the cdc_state table. Avoiding additional bootstrap call in that case will also help
        // us avoid unnecessary network calls.
        if (snapshotter.shouldSnapshot()) {
            LOGGER.info("Skipping bootstrap because snapshot has been taken so streaming will resume there onwards");
        } else {
            bootstrapTabletWithRetry(tabletPairList);
        }

        // This log while indicate that the connector has either bootstrapped the tablets or skipped
        // it so that streaming can begin now. This is added to indicate the tests or pipelines
        // waiting for the bootstrapping to finish so that they can start inserting data now.
        LOGGER.info("Beginning to poll the changes from the server");

        short retryCount = 0;

        // Helper internal variable to log GetChanges request at regular intervals.
        long lastLoggedTimeForGetChanges = System.currentTimeMillis();

        String curTabletId = "";
        while (context.isRunning() && retryCount <= connectorConfig.maxConnectorRetries()) {
            try {
                while (context.isRunning() && (offsetContext.getStreamingStoppingLsn() == null ||
                        (lastCompletelyProcessedLsn.compareTo(offsetContext.getStreamingStoppingLsn()) < 0))) {
                    // Pause for the specified duration before asking for a new set of changes from the server
                    LOGGER.debug("Pausing for {} milliseconds before polling further", connectorConfig.cdcPollIntervalms());
                    final Metronome pollIntervalMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.cdcPollIntervalms()), Clock.SYSTEM);
                    pollIntervalMetronome.pause();

                    if (this.connectorConfig.cdcLimitPollPerIteration()
                            && queue.remainingCapacity() < queue.totalCapacity()) {
                        LOGGER.debug("Queue has {} items. Skipping", queue.totalCapacity() - queue.remainingCapacity());
                        continue;
                    }

                    for (Pair<String, String> entry : tabletPairList) {
                        final String tabletId = entry.getValue();
                        curTabletId = entry.getValue();
                        YBPartition part = new YBPartition(entry.getKey() /* tableId */, tabletId, false /* colocated */);

                      OpId cp = offsetContext.lsn(part);

                      YBTable table = tableIdToTable.get(entry.getKey());

                      if (LOGGER.isDebugEnabled()
                          || (connectorConfig.logGetChanges() && System.currentTimeMillis() >= (lastLoggedTimeForGetChanges + connectorConfig.logGetChangesIntervalMs()))) {
                        LOGGER.info("Requesting changes for tablet {} from OpId {} for table {}",
                                    tabletId, cp, table.getName());
                        lastLoggedTimeForGetChanges = System.currentTimeMillis();
                      }

                      // Check again if the thread has been interrupted.
                      if (!context.isRunning()) {
                        LOGGER.info("Connector has been stopped");
                        break;
                      }

                      GetChangesResponse response = null;

                      if (schemaNeeded.get(part.getId())) {
                        LOGGER.debug("Requesting schema for tablet: {}", tabletId);
                      }

                      try {
                        response = this.syncClient.getChangesCDCSDK(
                            table, streamId, tabletId, cp.getTerm(), cp.getIndex(), cp.getKey(),
                            cp.getWrite_id(), cp.getTime(), schemaNeeded.get(part.getId()),
                            taskContext.shouldEnableExplicitCheckpointing() ? tabletToExplicitCheckpoint.get(part.getId()) : null);
                      } catch (CDCErrorException cdcException) {
                        // Check if exception indicates a tablet split.
                        LOGGER.debug("Code received in CDCErrorException: {}", cdcException.getCDCError().getCode());
                        if (cdcException.getCDCError().getCode() == Code.TABLET_SPLIT || cdcException.getCDCError().getCode() == Code.INVALID_REQUEST) {
                            LOGGER.info("Encountered a tablet split on tablet {}, handling it gracefully", tabletId);
                            if (LOGGER.isDebugEnabled()) {
                                cdcException.printStackTrace();
                            }

                            handleTabletSplit(cdcException, tabletPairList, offsetContext, streamId, schemaNeeded);

                            // Break out of the loop so that the iteration can start afresh on the modified list.
                            break;
                        } else {
                            LOGGER.warn("Throwing error because error code did not match. Expected split code: {}, code received: {}", Code.TABLET_SPLIT, cdcException.getCDCError().getCode());
                            throw cdcException;
                        }
                      }

                        LOGGER.debug("Processing {} records from getChanges call",
                                response.getResp().getCdcSdkProtoRecordsList().size());
                        for (CdcService.CDCSDKProtoRecordPB record : response
                                .getResp()
                                .getCdcSdkProtoRecordsList()) {
                            CdcService.RowMessage m = record.getRowMessage();
                            YbProtoReplicationMessage message = new YbProtoReplicationMessage(
                                    m, this.yugabyteDBTypeRegistry);

                            String pgSchemaNameInRecord = m.getPgschemaName();

                            // This is a hack to skip tables in case of colocated tables
                            TableId tempTid = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaNameInRecord);
                            if (!message.isDDLMessage() && !message.isTransactionalMessage()
                                  && !new Filters(connectorConfig).tableFilter().isIncluded(tempTid)) {
                                continue;
                            }

                            final OpId lsn = new OpId(record.getCdcSdkOpId().getTerm(),
                                    record.getCdcSdkOpId().getIndex(),
                                    record.getCdcSdkOpId().getWriteIdKey().toByteArray(),
                                    record.getCdcSdkOpId().getWriteId(),
                                    response.getSnapshotTime());

                            if (message.isLastEventForLsn()) {
                                lastCompletelyProcessedLsn = lsn;
                            }

                            try {
                                // Tx BEGIN/END event
                                if (message.isTransactionalMessage()) {
                                    if (!connectorConfig.shouldProvideTransactionMetadata()) {
                                        LOGGER.debug("Received transactional message {}", record);
                                        // Don't skip on BEGIN message as it would flush LSN for the whole transaction
                                        // too early
                                        if (message.getOperation() == Operation.BEGIN) {
                                            LOGGER.debug("LSN in case of BEGIN is " + lsn);

                                            recordsInTransactionalBlock.put(part.getId(), 0);
                                            beginCountForTablet.merge(part.getId(), 1, Integer::sum);
                                        }
                                        if (message.getOperation() == Operation.COMMIT) {
                                            LOGGER.debug("LSN in case of COMMIT is " + lsn);
                                            offsetContext.updateWalPosition(part, lsn, lastCompletelyProcessedLsn, message.getCommitTime(),
                                                    String.valueOf(message.getTransactionId()), null, null/* taskContext.getSlotXmin(connection) */);
                                            commitMessage(part, offsetContext, lsn);

                                            if (recordsInTransactionalBlock.containsKey(part.getId())) {
                                                if (recordsInTransactionalBlock.get(part.getId()) == 0) {
                                                    LOGGER.debug("Records in the transactional block of transaction: {}, with LSN: {}, for tablet {} are 0",
                                                                message.getTransactionId(), lsn, part.getId());
                                                } else {
                                                    LOGGER.debug("Records in the transactional block transaction: {}, with LSN: {}, for tablet {}: {}",
                                                                 message.getTransactionId(), lsn, part.getId(), recordsInTransactionalBlock.get(part.getId()));
                                                }
                                            } else if (beginCountForTablet.get(part.getId()).intValue() == 0) {
                                                throw new DebeziumException("COMMIT record encountered without a preceding BEGIN record");
                                            }

                                            recordsInTransactionalBlock.remove(part.getId());
                                            beginCountForTablet.merge(part.getId(), -1, Integer::sum);
                                        }
                                        continue;
                                    }

                                    if (message.getOperation() == Operation.BEGIN) {
                                        LOGGER.debug("LSN in case of BEGIN is " + lsn);
                                        dispatcher.dispatchTransactionStartedEvent(part, message.getTransactionId(), offsetContext);

                                        recordsInTransactionalBlock.put(part.getId(), 0);
                                        beginCountForTablet.merge(part.getId(), 1, Integer::sum);
                                    }
                                    else if (message.getOperation() == Operation.COMMIT) {
                                        LOGGER.debug("LSN in case of COMMIT is " + lsn);
                                        offsetContext.updateWalPosition(part, lsn, lastCompletelyProcessedLsn, message.getCommitTime(),
                                                String.valueOf(message.getTransactionId()), null, null/* taskContext.getSlotXmin(connection) */);
                                        commitMessage(part, offsetContext, lsn);
                                        dispatcher.dispatchTransactionCommittedEvent(part, offsetContext);

                                        if (recordsInTransactionalBlock.containsKey(part.getId())) {
                                            if (recordsInTransactionalBlock.get(part.getId()) == 0) {
                                                LOGGER.debug("Records in the transactional block of transaction: {}, with LSN: {}, for tablet {} are 0",
                                                            message.getTransactionId(), lsn, part.getId());
                                            } else {
                                                LOGGER.debug("Records in the transactional block transaction: {}, with LSN: {}, for tablet {}: {}",
                                                             message.getTransactionId(), lsn, part.getId(), recordsInTransactionalBlock.get(part.getId()));
                                            }
                                        } else if (beginCountForTablet.get(part.getId()).intValue() == 0) {
                                            throw new DebeziumException("COMMIT record encountered without a preceding BEGIN record");
                                        }

                                        recordsInTransactionalBlock.remove(part.getId());
                                        beginCountForTablet.merge(part.getId(), -1, Integer::sum);
                                    }
                                    maybeWarnAboutGrowingWalBacklog(true);
                                }
                                else if (message.isDDLMessage()) {
                                    LOGGER.debug("Received DDL message {}", message.getSchema().toString()
                                            + " the table is " + message.getTable());

                                    // If a DDL message is received for a tablet, we do not need its schema again
                                    schemaNeeded.put(part.getId(), Boolean.FALSE);

                                    TableId tableId = null;
                                    if (message.getOperation() != Operation.NOOP) {
                                        tableId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaNameInRecord);
                                        Objects.requireNonNull(tableId);
                                    }
                                    // Getting the table with the help of the schema.
                                    Table t = schema.tableForTablet(tableId, tabletId);
                                    if (YugabyteDBSchema.shouldRefreshSchema(t, message.getSchema())) {
                                        // If we fail to achieve the table, that means we have not specified correct schema information,
                                        // now try to refresh the schema.
                                        if (t == null) {
                                            LOGGER.info("Registering the schema for table {} tablet {} since it was not registered already", entry.getKey(), tabletId);
                                        } else {
                                            LOGGER.info("Refreshing the schema for table {} tablet {} because of mismatch in cached schema and received schema", entry.getKey(), tabletId);
                                        }
                                        schema.refreshSchemaWithTabletId(tableId, message.getSchema(), pgSchemaNameInRecord, tabletId);
                                    }
                                }
                                // DML event
                                else {
                                    TableId tableId = null;
                                    if (message.getOperation() != Operation.NOOP) {
                                        tableId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaNameInRecord);
                                        Objects.requireNonNull(tableId);
                                    }
                                    // If you need to print the received record, change debug level to info
                                    LOGGER.debug("Received DML record {}", record);

                                    offsetContext.updateWalPosition(part, lsn, lastCompletelyProcessedLsn, message.getCommitTime(),
                                            String.valueOf(message.getTransactionId()), tableId, null/* taskContext.getSlotXmin(connection) */);

                                    boolean dispatched = message.getOperation() != Operation.NOOP
                                            && dispatcher.dispatchDataChangeEvent(part, tableId, new YugabyteDBChangeRecordEmitter(part, offsetContext, clock, connectorConfig,
                                                    schema, connection, tableId, message, pgSchemaNameInRecord, tabletId, taskContext.isBeforeImageEnabled()));

                                    if (recordsInTransactionalBlock.containsKey(part.getId())) {
                                        recordsInTransactionalBlock.merge(part.getId(), 1, Integer::sum);
                                    }

                                    maybeWarnAboutGrowingWalBacklog(dispatched);
                                }
                            } catch (InterruptedException ie) {
                                LOGGER.error("Interrupted exception while processing change records", ie);
                                Thread.currentThread().interrupt();
                            }
                        }

                        probeConnectionIfNeeded();

                        if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
                            // During catch up streaming, the streaming phase needs to hold a transaction open so that
                            // the phase can stream event up to a specific lsn and the snapshot that occurs after the catch up
                            // streaming will not lose the current view of data. Since we need to hold the transaction open
                            // for the snapshot, this block must not commit during catch up streaming.
                            // CDCSDK Find out why this fails : connection.commit();
                        }
                        OpId finalOpid = new OpId(
                                response.getTerm(),
                                response.getIndex(),
                                response.getKey(),
                                response.getWriteId(),
                                response.getSnapshotTime());
                        offsetContext.getSourceInfo(part).updateLastCommit(finalOpid);

                        LOGGER.debug("The final opid is " + finalOpid);
                    }
                    // Reset the retry count, because if flow reached at this point, it means that the connection
                    // has succeeded
                    retryCount = 0;
                }
            } catch (Exception e) {
                ++retryCount;
                // If the retry limit is exceeded, log an error with a description and throw the exception.
                if (retryCount > connectorConfig.maxConnectorRetries()) {
                    LOGGER.error("Too many errors while trying to get the changes from server for tablet: {}. All {} retries failed.", curTabletId, connectorConfig.maxConnectorRetries());
                    throw e;
                }

                // If there are retries left, perform them after the specified delay.
                LOGGER.warn("Error while trying to get the changes from the server; will attempt retry {} of {} after {} milli-seconds. Exception message: {}",
                        retryCount, connectorConfig.maxConnectorRetries(), connectorConfig.connectorRetryDelayMs(), e.getMessage());
                LOGGER.warn("Stacktrace", e);

                try {
                    final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
                    retryMetronome.pause();
                }
                catch (InterruptedException ie) {
                    LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void searchWalPosition(ChangeEventSourceContext context, final ReplicationStream stream, final WalPositionLocator walPosition)
            throws SQLException, InterruptedException {
        AtomicReference<OpId> resumeLsn = new AtomicReference<>();
        int noMessageIterations = 0;

        LOGGER.info("Searching for WAL resume position");
        while (context.isRunning() && resumeLsn.get() == null) {

            boolean receivedMessage = stream.readPending(message -> {
                final OpId lsn = stream.lastReceivedLsn();
                resumeLsn.set(walPosition.resumeFromLsn(lsn, message).orElse(null));
            });

            if (receivedMessage) {
                noMessageIterations = 0;
            }
            else {
                noMessageIterations++;
                if (noMessageIterations >= THROTTLE_NO_MESSAGE_BEFORE_PAUSE) {
                    noMessageIterations = 0;
                    pauseNoMessage.sleepWhen(true);
                }
            }

            probeConnectionIfNeeded();
        }
        LOGGER.info("WAL resume position '{}' discovered", resumeLsn.get());
    }

    private void probeConnectionIfNeeded() throws SQLException {
        // CDCSDK Find out why it fails.
        // if (connectionProbeTimer.hasElapsed()) {
        // connection.prepareQuery("SELECT 1");
        // connection.commit();
        // }
    }

    private void commitMessage(YBPartition partition, YugabyteDBOffsetContext offsetContext,
                               final OpId lsn)
            throws SQLException, InterruptedException {
        lastCompletelyProcessedLsn = lsn;
        offsetContext.updateCommitPosition(lsn, lastCompletelyProcessedLsn);
        maybeWarnAboutGrowingWalBacklog(false);
        // dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
    }

    /**
     * If we receive change events but all of them get filtered out, we cannot
     * commit any new offset with Apache Kafka. This in turn means no LSN is ever
     * acknowledged with the replication slot, causing any ever growing WAL backlog.
     * <p>
     * This situation typically occurs if there are changes on the database server,
     * (e.g. in an excluded database), but none of them is in table.include.list.
     * To prevent this, heartbeats can be used, as they will allow us to commit
     * offsets also when not propagating any "real" change event.
     * <p>
     * The purpose of this method is to detect this situation and log a warning
     * every {@link #GROWING_WAL_WARNING_LOG_INTERVAL} filtered events.
     *
     * @param dispatched
     *            Whether an event was sent to the broker or not
     */
    private void maybeWarnAboutGrowingWalBacklog(boolean dispatched) {
        if (dispatched) {
            numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
        }
        else {
            numberOfEventsSinceLastEventSentOrWalGrowingWarning++;
        }

        if (numberOfEventsSinceLastEventSentOrWalGrowingWarning > GROWING_WAL_WARNING_LOG_INTERVAL && !dispatcher.heartbeatsEnabled()) {
            LOGGER.warn("Received {} events which were all filtered out, so no offset could be committed. "
                    + "This prevents the replication slot from acknowledging the processed WAL offsets, "
                    + "causing a growing backlog of non-removeable WAL segments on the database server. "
                    + "Consider to either adjust your filter configuration or enable heartbeat events "
                    + "(via the {} option) to avoid this situation.",
                    numberOfEventsSinceLastEventSentOrWalGrowingWarning, Heartbeat.HEARTBEAT_INTERVAL_PROPERTY_NAME);

            numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
        }
    }

    /**
     * For EXPLICIT checkpointing, the following code flow is used:<br><br>
     * 1. Kafka Connect invokes the callback {@code commit()} which further invokes
     * {@code commitOffset(offsets)} in the Debezium API <br><br>
     * 2. Both the streaming and snapshot change event source classes maintain a map
     * {@code tabletToExplicitCheckpoint} which stores the offsets sent by Kafka Connect. <br><br>
     * 3. So when the connector gets the acknowledgement back by saying that Kafka has received
     * records till certain offset, we update the value in {@code tabletToExplicitCheckpoint} <br><br>
     * 4. While making the next `GetChanges` call, we pass the value from the map
     * {@code tabletToExplicitCheckpoint} for the relevant tablet and hence the server then takes
     * care of updating those checkpointed values in the {@code cdc_state} table <br><br>
     * @param offset a map containing the {@link OpId} information for all the tablets
     */
    @Override
    public void commitOffset(Map<String, ?> offset) {
        if (!taskContext.shouldEnableExplicitCheckpointing()) {
            return;
        }

        try {
            LOGGER.info("Committing offsets on server");

            for (Map.Entry<String, ?> entry : offset.entrySet()) {
                // TODO: The transaction_id field is getting populated somewhere and see if it can
                // be removed or blocked from getting added to this map.
                if (!entry.getKey().equals("transaction_id")) {
                    LOGGER.debug("Tablet: {} OpId: {}", entry.getKey(), entry.getValue());

                    // Parse the string to get the OpId object.
                    OpId tempOpId = OpId.valueOf((String) entry.getValue());
                    this.tabletToExplicitCheckpoint.put(entry.getKey(), tempOpId.toCdcSdkCheckpoint());

                    LOGGER.debug("Committed checkpoint on server for stream ID {} tablet {} with term {} index {}",
                                this.connectorConfig.streamId(), entry.getKey(), tempOpId.getTerm(), tempOpId.getIndex());
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Unable to update the explicit checkpoint map", e);
        }
    }

    /**
     * Returns whether the current streaming phase is running a catch up streaming
     * phase that runs before a snapshot. This is useful for transaction
     * management.
     *
     * During pre-snapshot catch up streaming, we open the snapshot transaction
     * early and hold the transaction open throughout the pre snapshot catch up
     * streaming phase so that we know where to stop streaming and can start the
     * snapshot phase at a consistent location. This is opposed the regular streaming,
     * where we do not a lingering open transaction.
     *
     * @return true if the current streaming phase is performing catch up streaming
     */
    private boolean isInPreSnapshotCatchUpStreaming(YugabyteDBOffsetContext offsetContext) {
        return offsetContext.getStreamingStoppingLsn() != null;
    }

    @FunctionalInterface
    public static interface PgConnectionSupplier {
        BaseConnection get() throws SQLException;
    }

    /**
     * Get the entry from the tablet pair list corresponding to the given tablet ID. This function
     * is helpful at the time of tablet split where we know the tablet ID of the tablet which has
     * been split and now we want to remove the corresponding pair from the polling list of
     * table-tablet pairs.
     * @param tabletPairList list of table-tablet pair to poll from
     * @param tabletId the tablet ID to match with
     * @return a pair of table-tablet IDs which matches the provided tablet ID
     */
    private Pair<String, String> getEntryToDelete(List<Pair<String,String>> tabletPairList, String tabletId) {
        for (Pair<String, String> entry : tabletPairList) {
            if (entry.getValue().equals(tabletId)) {
                return entry;
            }
        }

        return null;
    }

    /**
     * Parse the message from the {@link CDCErrorException} to obtain the tablet ID of the tablet.
     * which has been split
     * @param message the exception message to parse
     * @return the tablet UUID of the tablet which has been split
     */
    private String getTabletIdFromSplitMessage(String message) {
        // Note that the message is of the form: Tablet Split detected on <tablet-ID>
        // So the last element is the tablet ID to be split.
        String[] splitWords = message.split("\\s+");
        return splitWords[splitWords.length - 1];
    }

    /**
     * Add the tablet from the provided tablet checkpoint pair to the list of tablets to poll from
     * if it is not present there
     * @param tabletPairList the list of tablets to poll from - list having Pair<tableId, tabletId>
     * @param pair the tablet checkpoint pair
     * @param tableId table UUID of the table to which the tablet belongs
     * @param offsetContext the offset context having the lsn info
     * @param schemaNeeded map of flags indicating whether we need the schema for a tablet or not
     */
    private void addTabletIfNotPresent(List<Pair<String,String>> tabletPairList,
                                       TabletCheckpointPair pair,
                                       String tableId,
                                       YugabyteDBOffsetContext offsetContext,
                                       Map<String, Boolean> schemaNeeded) {
        String tabletId = pair.getTabletLocations().getTabletId().toStringUtf8();
        ImmutablePair<String, String> tableTabletPair =
          new ImmutablePair<String, String>(tableId, tabletId);

        if (!tabletPairList.contains(tableTabletPair)) {
            tabletPairList.add(tableTabletPair);

            // This flow will be executed in case of tablet split only and since tablet split
            // is not possible on colocated tables, it is safe to assume that the tablets here
            // would be all non-colocated.
            YBPartition p = new YBPartition(tableId, tabletId, false /* colocated */);
            offsetContext.initSourceInfo(p, this.connectorConfig,
                                         OpId.from(pair.getCdcSdkCheckpoint()));

            LOGGER.info("Initialized offset context for tablet {} with OpId {}", tabletId, OpId.from(pair.getCdcSdkCheckpoint()));

            // Add the flag to indicate that we need the schema for the new tablets so that the schema can be registered.
            schemaNeeded.put(p.getId(), Boolean.TRUE);
        }
    }

    private void handleTabletSplit(CDCErrorException cdcErrorException,
                                   List<Pair<String,String>> tabletPairList,
                                   YugabyteDBOffsetContext offsetContext,
                                   String streamId,
                                   Map<String, Boolean> schemaNeeded) throws Exception {
        // Obtain the tablet ID of the split tablet from the message.
        String splitTabletId = getTabletIdFromSplitMessage(cdcErrorException.getMessage());
        LOGGER.info("Removing the tablet {} from the list to get the changes since it has been split", splitTabletId);

        Pair<String, String> entryToBeDeleted = getEntryToDelete(tabletPairList, splitTabletId);
        Objects.requireNonNull(entryToBeDeleted);

        String tableId = entryToBeDeleted.getKey();

        GetTabletListToPollForCDCResponse getTabletListResponse =
          getTabletListResponseWithRetry(
              this.syncClient.openTableByUUID(tableId),
              streamId,
              tableId,
              splitTabletId);
        
        Objects.requireNonNull(getTabletListResponse);

        // Remove the entry with the tablet which has been split.
        boolean removeSuccessful = tabletPairList.remove(entryToBeDeleted);

        // Remove the corresponding entry to indicate that we don't need the schema now.
        schemaNeeded.remove(entryToBeDeleted.getValue());

        // Log a warning if the element cannot be removed from the list.
        if (!removeSuccessful) {
            LOGGER.warn("Failed to remove the entry table {} - tablet {} from tablet pair list after split, will try once again", entryToBeDeleted.getKey(), entryToBeDeleted.getValue());

            if (!tabletPairList.remove(entryToBeDeleted)) {
                String exceptionMessageFormat = "Failed to remove the entry table {} - tablet {} from the tablet pair list after split";
                throw new RuntimeException(String.format(exceptionMessageFormat, entryToBeDeleted.getKey(), entryToBeDeleted.getValue()));
            }
        }

        for (TabletCheckpointPair pair : getTabletListResponse.getTabletCheckpointPairList()) {
            addTabletIfNotPresent(tabletPairList, pair, tableId, offsetContext, schemaNeeded);
        }
    }

    /**
     * Get the children tablets of the specified tablet which has been split. This API will retry
     * until the retry limit has been hit.
     * @param ybTable the {@link YBTable} object to which the split tablet belongs
     * @param streamId the DB stream ID being used for polling
     * @param tableId table UUID of the table to which the split tablet belongs
     * @param splitTabletId tablet UUID of the split tablet
     * @return {@link GetTabletListToPollForCDCResponse} containing the list of child tablets
     * @throws Exception when the retry limit has been hit or the metronome pause is interrupted
     */
    private GetTabletListToPollForCDCResponse getTabletListResponseWithRetry(
        YBTable ybTable, String streamId, String tableId, String splitTabletId) throws Exception {
        short retryCount = 0;
        while (retryCount <= connectorConfig.maxConnectorRetries()) {
            try {
                // Note that YBClient also retries internally if it encounters an error which can
                // be retried.
                GetTabletListToPollForCDCResponse response =
                    this.syncClient.getTabletListToPollForCdc(
                        ybTable,
                        streamId,
                        tableId,
                        splitTabletId);

                if (response.getTabletCheckpointPairListSize() != 2) {
                    LOGGER.warn("Found {} tablets for the parent tablet {}",
                            response.getTabletCheckpointPairListSize(), splitTabletId);
                    throw new Exception("Found unexpected ("
                            + response.getTabletCheckpointPairListSize()
                            + ") number of tablets while trying to fetch the children of parent "
                            + splitTabletId);
                }

                retryCount = 0;
                return response;
            } catch (Exception e) {
                ++retryCount;
                if (retryCount > connectorConfig.maxConnectorRetries()) {
                    LOGGER.error("Too many errors while trying to get children for split tablet {}", splitTabletId);
                    LOGGER.error("Error", e);
                    throw e;
                }

                LOGGER.warn("Error while trying to get the children for the split tablet {}, will retry attempt {} of {} after {} milliseconds",
                            splitTabletId, retryCount, connectorConfig.maxConnectorRetries(), connectorConfig.connectorRetryDelayMs());
                LOGGER.warn("Stacktrace", e);

                try {
                    final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
                    retryMetronome.pause();
                }
                catch (InterruptedException ie) {
                    LOGGER.warn("Connector retry sleep while pausing to get the children tablets for parent {} interrupted", splitTabletId);
                    LOGGER.warn("Exception for interruption", ie);
                    Thread.currentThread().interrupt();
                }
            }
        }

        // In ideal scenarios, this should NEVER be returned from this function.
        return null;
    }
}
