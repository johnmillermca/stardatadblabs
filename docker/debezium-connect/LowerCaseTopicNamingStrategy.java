package io.debezium.schema;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;

import java.util.Properties;

/**
 * LowerCaseTopicNamingStrategy
 *
 * Extends DefaultTopicNamingStrategy and lowercases every segment of the
 * data-change topic name produced by DefaultTopicNamingStrategy.
 *
 * Oracle stores identifiers in uppercase in its data dictionary, so without
 * this strategy the connector emits topics like:
 *   oracle.CACHE_TESTING.CUSTOMERS
 *
 * With this strategy it emits:
 *   oracle.cache_testing.customers
 *
 * Compatible with Debezium 2.7.x (debezium-core 2.7.4.Final).
 *
 * Usage in connector config:
 *   "topic.naming.strategy": "io.debezium.schema.LowerCaseTopicNamingStrategy"
 *
 * Build (requires debezium-core-2.7.4.Final.jar and debezium-api-2.7.4.Final.jar
 * on the classpath — see build.sh):
 *
 *   cd docker/debezium-connect
 *   bash build.sh
 *
 * The compiled JAR is committed to git so the Docker build does not need a JDK.
 */
public class LowerCaseTopicNamingStrategy extends DefaultTopicNamingStrategy {

    public LowerCaseTopicNamingStrategy(Properties props) {
        super(props);
    }

    /**
     * Factory method called by Debezium via reflection when constructing the
     * strategy through CommonConnectorConfig.getTopicNamingStrategy().
     */
    public static LowerCaseTopicNamingStrategy create(CommonConnectorConfig config) {
        return new LowerCaseTopicNamingStrategy(config.getConfig().asProperties());
    }

    /**
     * Returns the data-change topic name with every character lowercased.
     *
     * DefaultTopicNamingStrategy.dataChangeTopic() returns:
     *   {prefix}.{schema}.{table}   e.g.  oracle.CACHE_TESTING.CUSTOMERS
     * This override lowercases the whole string to:
     *   oracle.cache_testing.customers
     */
    @Override
    public String dataChangeTopic(DataCollectionId id) {
        return super.dataChangeTopic(id).toLowerCase();
    }
}
