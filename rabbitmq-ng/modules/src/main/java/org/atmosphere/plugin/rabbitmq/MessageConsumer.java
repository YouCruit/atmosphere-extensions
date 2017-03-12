package org.atmosphere.plugin.rabbitmq;

public interface MessageConsumer {
    void consumeMessage(byte[] body);
}
