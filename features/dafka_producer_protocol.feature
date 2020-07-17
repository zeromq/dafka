Feature: Dafka producer protocol

  Scenario: Consumer requests missed non-acked message from producer
    Given a dafka producer for topic hello
    And the producer sent a RECORD message with content 'CONTENT'
    When the producer receives a FETCH message with sequence 0
    Then the producer will send a DIRECT_RECORD message with content 'CONTENT'

  Scenario: Producer sends HEAD messages at interval after first message was produced
