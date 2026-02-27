package it.water.connectors.kafka.api;

import it.water.connectors.kafka.model.ConnectorConfig;
import it.water.connectors.kafka.model.KafkaConnector;
import it.water.connectors.kafka.model.KafkaPermission;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Common management operations shared between KafkaConnectorApi and KafkaConnectorSystemApi.
 * Provides Kafka Admin operations (topics, ACLs) and Kafka Connect connector lifecycle management.
 *
 * This interface eliminates code duplication by defining operations that are exposed both
 * through the public API (with authorization) and system API (without authorization).
 */
public interface KafkaConnectorManagementApi {

    // =====================================================
    // Kafka Admin Operations - Topic Management
    // =====================================================

    /**
     * Creates a Kafka topic through the Kafka Admin client.
     *
     * @param topic topic name to create
     * @param numPartitions number of partitions to assign
     * @param numReplicas replication factor
     * @return Kafka admin create-topic result, or {@code null} if admin client is not available
     */
    CreateTopicsResult adminCreateTopic(String topic, int numPartitions, short numReplicas);

    /**
     * Creates multiple Kafka topics through the Kafka Admin client.
     *
     * @param topics topic names to create
     * @param numPartitions partitions per topic (same index as {@code topics})
     * @param numReplicas replication factor per topic (same index as {@code topics})
     * @return Kafka admin create-topics result, or {@code null} if admin client is not available
     */
    CreateTopicsResult adminCreateTopic(String[] topics, int[] numPartitions, short[] numReplicas);

    /**
     * Deletes Kafka topics through the Kafka Admin client.
     *
     * @param topics topics to delete
     * @return Kafka admin delete-topics result, or {@code null} if admin client is not available
     */
    DeleteTopicsResult adminDropTopic(List<String> topics);

    // =====================================================
    // Kafka Admin Operations - ACL Management
    // =====================================================

    /**
     * Adds Kafka ACLs for a user on the configured resources.
     *
     * @param username Kafka principal/user name
     * @param permissions map of permissions keyed by caller-defined identifiers
     * @return Kafka admin create-acls result, or {@code null} if admin client is not available
     */
    CreateAclsResult adminAddACLs(String username, Map<String, KafkaPermission> permissions);

    /**
     * Deletes Kafka ACLs for a user on the configured resources.
     *
     * @param username Kafka principal/user name
     * @param permissions map of permissions keyed by caller-defined identifiers
     * @return Kafka admin delete-acls result, or {@code null} if admin client is not available
     */
    DeleteAclsResult adminDeleteACLs(String username, Map<String, KafkaPermission> permissions);

    // =====================================================
    // Kafka Connect Connector Management
    // =====================================================

    /**
     * Creates a new Kafka Connect connector instance.
     *
     * @param instanceName connector instance name
     * @param config connector configuration payload
     * @return created connector representation
     * @throws IOException if Kafka Connect responds with an error or the response cannot be parsed
     */
    KafkaConnector addNewConnector(String instanceName, ConnectorConfig config) throws IOException;

    /**
     * Deletes a Kafka Connect connector instance.
     *
     * @param instanceName connector instance name
     * @param deleteKafkaTopic whether the corresponding Kafka topic must also be deleted
     * @throws IOException if Kafka Connect responds with an error
     */
    void deleteConnector(String instanceName, boolean deleteKafkaTopic) throws IOException;

    /**
     * Retrieves a Kafka Connect connector instance configuration/status.
     *
     * @param instanceName connector instance name
     * @return connector representation
     * @throws IOException if Kafka Connect responds with an error or the response cannot be parsed
     */
    KafkaConnector getConnector(String instanceName) throws IOException;

    /**
     * Lists all Kafka Connect connector instance names.
     *
     * @return connector names
     * @throws IOException if Kafka Connect responds with an error or the response cannot be parsed
     */
    List<String> listConnectors() throws IOException;

    /**
     * Updates a Kafka Connect connector instance configuration.
     *
     * @param instanceName connector instance name
     * @param config connector configuration payload
     * @return updated connector representation
     * @throws IOException if Kafka Connect responds with an error or the response cannot be parsed
     */
    KafkaConnector updateConnector(String instanceName, ConnectorConfig config) throws IOException;
}
