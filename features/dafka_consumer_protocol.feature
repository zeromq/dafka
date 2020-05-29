Feature: Dafka consumer protocol

  Scenario: STORE-HELLO -> CONSUMER-HELLO without subscription
    Given a dafka consumer with offset reset earliest
    And no subscriptions
    When a STORE-HELLO command is send by a store
    Then the consumer responds with CONSUMER-HELLO containing 0 topics

  Scenario: STORE-HELLO -> CONSUMER-HELLO with subscription
    Given a dafka consumer with offset reset earliest
    And a subscription to topic hello
    When a STORE-HELLO command is send by a store
    Then the consumer responds with CONSUMER-HELLO containing 1 topic

  Scenario: Consumer requests partition heads when joining
    Given a dafka consumer with offset reset earliest
    When the consumer subscribes to topic hello
    Then the consumer will send a GET_HEADS message for topic hello

  Scenario: First record for topic with offset reset earliest
    Given a dafka consumer with offset reset earliest
    And a subscription to topic hello
    When a RECORD message with sequence 1 and content 'CONTENT' is send on topic hello
    Then the consumer will send a FETCH message for topic hello with sequence 0
    When a RECORD message with sequence 0 and content 'CONTENT' is send on topic hello
    Then a consumer_msg is send to the user with topic hello and content 'CONTENT'

  Scenario: First record for topic with offset reset latest
    Given a dafka consumer with offset reset latest
    And a subscription to topic hello
    When a RECORD message with sequence 2 and content 'CONTENT' is send on topic hello
    Then a consumer_msg is send to the user with topic hello and content 'CONTENT'
