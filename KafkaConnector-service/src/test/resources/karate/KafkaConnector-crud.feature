# Generated with Water Generator
# The Goal of feature test is to ensure the correct format of json responses
# If you want to perform functional test please refer to ApiTest
Feature: Check KafkaConnector Rest Api Response

  Scenario: KafkaConnector module status endpoint
    Given header Accept = 'application/json'
    Given url serviceBaseUrl + '/water/kafka/module/status'
    When method GET
    Then status 200
