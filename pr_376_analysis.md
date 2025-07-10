# Analysis of PR #376 from yugabyte/debezium-connector-yugabytedb

## Overview

PR #376 represents a **major version upgrade** of the YugabyteDB Debezium connector from Debezium 1.9.5 to 3.1.3. This is a significant architectural modernization that brings the connector in line with the latest Debezium framework standards.

## Infrastructure & Dependencies Changes

### Docker & Build Environment
- **Base Image Update**: Upgraded from `debezium/connect:1.9.5.Final` to `quay.io/debezium/connect:3.1.3.Final`
- **Debezium Framework**: Major version jump from `1.9.5.Final` to `3.1.3.Final`
- **Java Runtime**: **Breaking Change** - Upgraded minimum Java version from 11 to 17
- **Dependencies**:
  - Jackson library: `2.15.0-rc1` → `2.16.2`
  - YugabyteDB client: `0.8.98-20250120.153122-1` → `0.8.105-SNAPSHOT`

### Maven Configuration
```xml
<!-- Key version changes in pom.xml -->
<maven.compiler.source>17</maven.compiler.source>
<maven.compiler.target>17</maven.compiler.target>
<jdk.min.version>17</jdk.min.version>
```

## Major API & Architecture Changes

### 1. Topic Naming Strategy Modernization
**Before (Debezium 1.x)**:
```java
TopicSelector<TableId> topicSelector = YugabyteDBTopicSelector.create(connectorConfig);
```

**After (Debezium 3.x)**:
```java
TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
```

### 2. Schema Name Adjuster Migration
- **Package Change**: `io.debezium.util.SchemaNameAdjuster` → `io.debezium.schema.SchemaNameAdjuster`
- **Usage Pattern**: Direct usage of `connectorConfig.schemaNameAdjuster()` instead of creating adjusters

### 3. Data Collection ID Updates
- **Import Change**: `io.debezium.schema.DataCollectionId` → `io.debezium.spi.schema.DataCollectionId`
- **Impact**: Affects all schema and event handling components

## Snapshot Handling Refactor

### New Snapshotter Service Architecture
```java
// Old pattern
private final Snapshotter snapshotter;

// New pattern  
private final SnapshotterService snapshotterService;
final Snapshotter snapshotter = snapshotterService.getSnapshotter();
```

### Enhanced Snapshot Locking
- **New Enum**: `SnapshotLockingMode` with values `SHARED`, `NONE`, `CUSTOM`
- **Configuration**: `snapshot.locking.mode` parameter added
- **Default**: `NONE` (no table locks during snapshot)

### Service Provider Interfaces
New SPI implementations added:
- `NoSnapshotLock` - Lock-free snapshot mode
- `SelectAllSnapshotQuery` - Default snapshot query handling

## Connection Management Modernization

### Connection Factory Pattern
```java
// New connection factory approach
MainConnectionProvidingConnectionFactory<YugabyteDBConnection> connectionFactory = 
    new DefaultMainConnectionProvidingConnectionFactory<>(() -> 
        new YugabyteDBConnection(connectorConfig.getJdbcConfig(), valueConverterBuilder, YugabyteDBConnection.CONNECTION_GENERAL));
```

### Bean Registry Integration
```java
// Manual bean registration for service discovery
connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
```

## Event Processing & Schema Enhancements

### Transaction Handling Updates
- **Enhanced Methods**: Transaction events now include timestamp parameters
- **Monitoring**: Updated `YugabyteDBTransactionMonitor` with improved metadata handling
- **Context**: Modified transaction context management

### Field Name Strategy
```java
// Schema builder updates
YBTableSchemaBuilder(valueConverterProvider, schemaNameAdjuster, customConverterRegistry, 
                     sourceInfoSchema, fieldNamer, multiPartitionMode)
```

### Signal Processing
- **New Component**: `SignalProcessor<YBPartition, YugabyteDBOffsetContext>`
- **Notification Service**: `NotificationService` integration
- **Channel Support**: Source signal channel handling

## Error Handling & Metrics

### Error Handler Pattern
```java
// Updated constructor signature
YugabyteDBErrorHandler(YugabyteDBConnectorConfig connectorConfig, 
                       ChangeEventQueue<?> queue, 
                       ErrorHandler replacedErrorHandler)
```

### Enhanced Metrics
- **Snapshot Metrics**: Added pause/resume functionality
- **Streaming Metrics**: Lag reset capabilities
- **Partition Metrics**: Improved partition-level monitoring

## Testing Infrastructure Updates

### Engine Migration
```java
// Old testing pattern
EmbeddedEngine.create().using(config)

// New testing pattern  
DebeziumEngine.Builder<SourceRecord> builder = createEngineBuilder();
builder.using(config.asProperties())
```

### Configuration Updates
- **Engine Name**: `EmbeddedEngine.ENGINE_NAME` → `AsyncEmbeddedEngine.ENGINE_NAME`
- **Properties**: Configuration now uses `.asProperties()` method
- **Lifecycle**: Updated engine creation and management patterns

## Breaking Changes Summary

### Runtime Requirements
1. **Java 17+** - Applications must upgrade their Java runtime
2. **Container Base**: New Quay.io registry location for base images

### API Changes
1. **Topic Naming**: All topic selector references need updating
2. **Schema Adjusters**: Package imports must be updated
3. **Service Registration**: Bean registry pattern required for service discovery

### Configuration Updates
1. **New Parameters**: `snapshot.locking.mode` configuration option
2. **Service Providers**: SPI implementations for snapshot handling

## Migration Impact

### For Existing Deployments
- **Java Runtime**: Must upgrade to Java 17+
- **Container Images**: Update to use new Quay.io base images
- **Configuration**: Review and update connector configurations

### For Custom Extensions
- **API Updates**: Implement new interface patterns
- **Service Providers**: Update SPI implementations
- **Testing**: Migrate test infrastructure to new engine patterns

## Benefits of This Upgrade

1. **Modern Architecture**: Aligns with Debezium 3.x best practices
2. **Enhanced Performance**: Improved connection and resource management
3. **Better Monitoring**: Enhanced metrics and observability
4. **Flexible Snapshots**: More granular snapshot control options
5. **Future Compatibility**: Prepares for upcoming Debezium features

## Compatibility Notes

- **Backward Compatible**: User-facing configuration remains largely unchanged
- **Forward Compatible**: Positions connector for future Debezium updates
- **Service Discovery**: Enhanced plugin and service provider architecture

This upgrade represents a significant modernization effort that maintains functional compatibility while updating the internal architecture to leverage Debezium 3.x capabilities and performance improvements.