# Generated with Water Generator
# The goal of this feature test is keeping a lightweight REST smoke-check.
Feature: Check KafkaConnector status endpoint

  Scenario: KafkaConnector status endpoint is reachable
    Given header Content-Type = 'application/json'
    And header Accept = 'application/json'
    Given url serviceBaseUrl + '/water/kafka/module/status'
    When method GET
    Then status 200
    And match response contains 'Water Kafka Connector Module works!'
