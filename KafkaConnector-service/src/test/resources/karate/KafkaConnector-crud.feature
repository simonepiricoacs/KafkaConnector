# Generated with Water Generator
# The goal of this feature test is ensuring REST endpoint availability without requiring a live Kafka cluster.
Feature: Check KafkaConnector Rest Api Response

  Scenario: KafkaConnector module status endpoint
    Given header Content-Type = 'application/json'
    And header Accept = 'application/json'
    Given url serviceBaseUrl + '/water/kafka/module/status'
    When method GET
    Then status 200
    And match response contains 'Water Kafka Connector Module works!'
