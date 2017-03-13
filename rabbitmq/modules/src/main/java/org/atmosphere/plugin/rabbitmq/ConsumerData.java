package org.atmosphere.plugin.rabbitmq;

import java.util.List;

class ConsumerData {
    public static final ConsumerData EMPTY_CONSUMER_DATA = new ConsumerData(null, "", "");

    private final MessageConsumer listener;
    private final String queueName;
    private final String consumerTag;

    public ConsumerData(final MessageConsumer listener, final String queueName, final String consumerTag) {
	this.listener = listener;
	this.queueName = queueName;
        this.consumerTag = consumerTag;
    }

    public String getQueueName() {
        return queueName;
    }

    public MessageConsumer getListener() {
        return listener;
    }

    public String getConsumerTag() {
        return consumerTag;
    }
}
