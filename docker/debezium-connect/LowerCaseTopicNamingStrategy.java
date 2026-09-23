package io.debezium.schema;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.BoundedConcurrentHashMap;

import java.util.List;
import java.util.Properties;

/**
 * LowerCaseTopicNamingStrategy — Debezium 2.7.x
 *
 * Produces fully-lowercase data-change topic names so that Oracle's uppercase
 * identifiers (CACHE_TESTING.CUSTOMERS) are normalised to
 * oracle.cache_testing.customers.
 *
 * Why we override getTopicName() rather than dataChangeTopic():
 *   DefaultTopicNamingStrategy.dataChangeTopic() populates the shared
 *   BoundedConcurrentHashMap 'topicNames' cache via computeIfAbsent. Our
 *   super.dataChangeTopic() call correctly returns a lowercase string on the
 *   FIRST call but the cache stores the RAW (uppercase) intermediate result
 *   from the lambda — subsequent calls return the cached uppercase value and
 *   skip our .toLowerCase() entirely.
 *
 *   Solution: write directly into 'topicNames' with a lowercase-producing
 *   lambda so the cache itself stores the lowercase string from the first call.
 */
public class LowerCaseTopicNamingStrategy extends DefaultTopicNamingStrategy {

    public LowerCaseTopicNamingStrategy(Properties props) {
        super(props);
    }

    public static LowerCaseTopicNamingStrategy create(CommonConnectorConfig config) {
        return new LowerCaseTopicNamingStrategy(config.getConfig().asProperties());
    }

    @Override
    public String dataChangeTopic(DataCollectionId id) {
        // Build the raw topic name the same way DefaultTopicNamingStrategy does,
        // but store and return it in lowercase so the cache is primed correctly.
        List<String> parts = io.debezium.util.Collect.arrayListOf(prefix, id.databaseParts());
        String rawName = mkString(parts, delimiter);
        // Use the inherited topicNames cache but with a lowercase-producing function.
        // computeIfAbsent only calls the lambda on first access; subsequent calls
        // return the already-lowercase cached value.
        return topicNames.computeIfAbsent(id,
                ignored -> sanitizedTopicName(rawName).toLowerCase());
    }
}
