Feature: Dafka consumer protocol

  Scenario: STORE-HELLO -> CONSUMER-HELLO without subscription
    Given a dafka consumer with no subscriptions
    When a STORE-HELLO command is sent by a store
    Then the consumer responds with CONSUMER-HELLO containing 0 topics

  Scenario: STORE-HELLO -> CONSUMER-HELLO with subscription
    Given a dafka consumer with a subscription to topic hello
    When a STORE-HELLO command is sent by a store
    Then the consumer responds with CONSUMER-HELLO containing 1 topic
