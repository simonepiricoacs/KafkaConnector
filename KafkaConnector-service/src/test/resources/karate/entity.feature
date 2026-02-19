# Generated with Water Generator
# The Goal of feature test is to ensure the correct format of json responses
# If you want to perform functional test please refer to ApiTest
Feature: Check KafkaConnector Entity Rest Api Response

  Scenario: List KafkaConnector entities
    Given header Accept = 'application/json'
    Given url serviceBaseUrl + '/water/kafka'
    When method GET
    Then status 200
    And match response contains { results: '#[]' }
