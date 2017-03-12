package org.atmosphere.plugin.rabbitmq;

import java.util.ArrayList;
import java.util.List;

class ConsumerData {
    public static final ConsumerData EMPTY_CONSUMER_DATA = new ConsumerData("", "") {
        @Override
        public void addListener(final MessageConsumer messageConsumer) {
        }

        @Override
        public void removeListener(final MessageConsumer messageConsumer) {
        }

        @Override
        public boolean hasNoConsumers() {
            return false;
        }
    };


    private List<MessageConsumer> listeners = new ArrayList<>();
    private final String queueName;
    private final String consumerTag;

    public ConsumerData(final String queueName, final String consumerTag) {
        this.queueName = queueName;
        this.consumerTag = consumerTag;
    }

    public synchronized void addListener(MessageConsumer messageConsumer) {
        // Trick to avoid having to synchronize the list
        List<MessageConsumer> copy = new ArrayList<>(listeners);
        copy.add(messageConsumer);
        this.listeners = copy;
    }

    public synchronized void removeListener(MessageConsumer messageConsumer) {
        // Trick to avoid having to synchronize the list
        List<MessageConsumer> copy = new ArrayList<>(listeners);
        copy.remove(messageConsumer);
        listeners = copy;
    }

    public String getQueueName() {
        return queueName;
    }

    public boolean hasNoConsumers() {
        return listeners.isEmpty();
    }

    public List<MessageConsumer> getListeners() {
        return listeners;
    }

    public String getConsumerTag() {
        return consumerTag;
    }
}
