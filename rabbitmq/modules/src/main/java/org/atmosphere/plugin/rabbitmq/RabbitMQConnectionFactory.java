/*
 * Copyright 2017 Async-IO.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.atmosphere.plugin.rabbitmq;

import static org.atmosphere.plugin.rabbitmq.ConsumerData.EMPTY_CONSUMER_DATA;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.atmosphere.cpr.AtmosphereConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * RabbitMQ Connection Factory.
 *
 * @author Thibault Normand
 * @author Jean-Francois Arcand
 */
public class RabbitMQConnectionFactory {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQBroadcaster.class);

    private static RabbitMQConnectionFactory factory;

    public static final String PARAM_HOST = RabbitMQBroadcaster.class.getName() + ".host";
    public static final String PARAM_USER = RabbitMQBroadcaster.class.getName() + ".user";
    public static final String PARAM_PASS = RabbitMQBroadcaster.class.getName() + ".password";
    public static final String PARAM_EXCHANGE_TYPE = RabbitMQBroadcaster.class.getName() + ".exchange";
    public static final String PARAM_VHOST = RabbitMQBroadcaster.class.getName() + ".vhost";
    public static final String PARAM_PORT = RabbitMQBroadcaster.class.getName() + ".port";
    public static final String PARAM_USE_SSL = RabbitMQBroadcaster.class.getName() + ".ssl";

    private String exchangeName;
    private Connection connection;
    private Channel channel;
    private DefaultConsumer consumer;

    private final ConcurrentHashMap<String, ConsumerData> listeners = new ConcurrentHashMap<>();

    private volatile int threadStartCount = 1;
    private ExecutorService executorService;

    public RabbitMQConnectionFactory(AtmosphereConfig config) {

        String exchange = config.getInitParameter(PARAM_EXCHANGE_TYPE, "topic");

        boolean useSsl = config.getInitParameter(PARAM_USE_SSL, false);

        exchangeName = "atmosphere." + exchange;
        config.shutdownHook(this::shutdown);

        try {
            logger.debug("Create Connection Factory");
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setUsername(config.getInitParameter(PARAM_USER, "guest"));
            connectionFactory.setPassword(config.getInitParameter(PARAM_PASS, "guest"));
            connectionFactory.setVirtualHost(config.getInitParameter(PARAM_VHOST, "/"));
            connectionFactory.setHost(config.getInitParameter(PARAM_HOST, "127.0.0.1"));
            connectionFactory.setPort(config.getInitParameter(PARAM_PORT, useSsl ? 5671 : 5672));
            if (useSsl) {
                connectionFactory.useSslProtocol();
            }

            logger.debug("Try to acquire a connection ...");
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
            channel.addShutdownListener(this::shutdownCompleted);

            logger.debug("Topic creation '{}'...", exchangeName);
            channel.exchangeDeclare(exchangeName, exchange);

            consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(final String consumerTag, final Envelope envelope, final BasicProperties properties, final byte[] body) throws IOException {
                    executorService.submit(() ->deliver(envelope.getRoutingKey(), body));
                }
            };
            executorService = Executors.newCachedThreadPool((runnable) -> new Thread(runnable, getClass().getSimpleName() + " #" + threadStartCount++));
        } catch (Exception e) {
            String msg = "Unable to configure RabbitMQBroadcaster";
            logger.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    private void deliver(final String routingKey, final byte[] body) {
        String message = new String(body);
        ConsumerData consumerData = listeners.getOrDefault(routingKey, EMPTY_CONSUMER_DATA);
        if (consumerData.getListener() != null) {
            consumerData.getListener().consumeMessage(message);
        }
    }

    static RabbitMQConnectionFactory getFactory(AtmosphereConfig config) {
        // No need to synchronize here as the first Broadcaster created is at startup.
        if (factory == null) {
            factory = new RabbitMQConnectionFactory(config);
        }
        return factory;
    }

    private void shutdown() {
        try {
            executorService.shutdown();
            channel.close();
            connection.close();
        } catch (IOException | TimeoutException e) {
            logger.trace("", e);
        }
    }

    private void shutdownCompleted(ShutdownSignalException cause) {
        logger.info("Recieved shutdownCompleted", cause);
    }

    public synchronized void deregisterListener(final String id, final MessageConsumer messageConsumer) throws IOException {
        ConsumerData consumerData = listeners.getOrDefault(id, EMPTY_CONSUMER_DATA);
        String queueName = consumerData.getQueueName();
        logger.debug("Delete queue {}", consumerData.getQueueName());
        channel.queueUnbind(queueName, exchangeName, id);
        channel.basicCancel(consumerData.getConsumerTag());
        channel.queueDelete(queueName);
    }

    public synchronized void registerListener(final String id, final MessageConsumer messageConsumer) {
        if (listeners.containsKey(id)) {
            throw new IllegalArgumentException("Really bad. REVERT!");
	} else {
	    listeners.put(id, createConsumerData(id, messageConsumer));
	}
    }

    private ConsumerData createConsumerData(final String id, final MessageConsumer messageConsumer) {
        try {
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, exchangeName, id);
            String consumerTag = channel.basicConsume(queueName, true, consumer);
            return new ConsumerData(messageConsumer, queueName, consumerTag);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }

    public void push(final String id, final String body) throws IOException {
        channel.basicPublish(exchangeName, id, MessageProperties.PERSISTENT_TEXT_PLAIN, body.getBytes());
    }


}
