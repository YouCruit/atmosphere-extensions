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

import static java.util.Collections.emptyList;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.atmosphere.cpr.AtmosphereConfig;
import org.atmosphere.cpr.Deliver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import be.olsson.bencoder.Bdecoder;
import be.olsson.bencoder.Bencoder;

/**
 * RabbitMQ Connection Factory.
 *
 * @author Thibault Normand
 * @author Jean-Francois Arcand
 */
public class RabbitMQConnectionFactory implements AtmosphereConfig.ShutdownHook, ShutdownListener {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQNGBroadcaster.class);

    private static RabbitMQConnectionFactory factory;

    public static final String KEY_MESSAGE = "message";
    public static final String KEY_DESTINATION = "dest";
    public static final String KEY_SOURCE = "from";
    public static final String PARAM_EXCHANGE_INCOMING_ROUTING_KEY = RabbitMQNGBroadcaster.class.getName() + ".incomingRoutingKey";
    public static final String PARAM_EXCHANGE_OUTGOING_ROUTING_KEY = RabbitMQNGBroadcaster.class.getName() + ".outgoingRoutingKey";
    public static final String PARAM_HOST = RabbitMQNGBroadcaster.class.getName() + ".host";
    public static final String PARAM_USER = RabbitMQNGBroadcaster.class.getName() + ".user";
    public static final String PARAM_PASS = RabbitMQNGBroadcaster.class.getName() + ".password";
    public static final String PARAM_VHOST = RabbitMQNGBroadcaster.class.getName() + ".vhost";
    public static final String PARAM_PORT = RabbitMQNGBroadcaster.class.getName() + ".port";
    public static final String PARAM_USE_SSL = RabbitMQNGBroadcaster.class.getName() + ".ssl";
    public static final String EXCHANGE_NAME = "atmosphere.topic";

    private final String outgoingRoutingKey;
    private final Connection connection;
    private final Channel channel;
    private final DefaultConsumer consumer;
    private final Bencoder bencoder = new Bencoder();
    private final Bdecoder bdecoder;

    private volatile int threadStartCount = 1;
    private final ExecutorService executorService = Executors.newCachedThreadPool((runnable) -> new Thread(runnable, getClass().getSimpleName() + " #" + threadStartCount++));

    private final ConcurrentHashMap<Id, List<MessageConsumer>> listeners = new ConcurrentHashMap<>();

    public RabbitMQConnectionFactory(AtmosphereConfig config) {
        bdecoder = new Bdecoder();
        bdecoder.setStringAsByteArray(true);
        String incomingRoutingKey = config.getInitParameter(PARAM_EXCHANGE_INCOMING_ROUTING_KEY, "to-atmosphere");
        outgoingRoutingKey = config.getInitParameter(PARAM_EXCHANGE_OUTGOING_ROUTING_KEY, "from-atmosphere");

        boolean useSsl = config.getInitParameter(PARAM_USE_SSL, false);

        logger.debug("Create Connection Factory");
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername(config.getInitParameter(PARAM_USER, "guest"));
        connectionFactory.setPassword(config.getInitParameter(PARAM_PASS, "guest"));
        connectionFactory.setVirtualHost(config.getInitParameter(PARAM_VHOST, "/"));
        connectionFactory.setHost(config.getInitParameter(PARAM_HOST, "127.0.0.1"));
        connectionFactory.setPort(config.getInitParameter(PARAM_PORT, useSsl ? 5671 : 5672));
        if (useSsl) {
            try {
                connectionFactory.useSslProtocol();
            } catch (GeneralSecurityException e) {
                String msg = "Unable to configure RabbitMQBroadcaster";
                logger.error(msg, e);
                throw new RuntimeException(msg, e);
            }
        }

        try {
            logger.debug("Try to acquire a connection ...");
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
            channel.addShutdownListener(this);

            logger.debug("Topic creation '{}'...", EXCHANGE_NAME);
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, EXCHANGE_NAME, incomingRoutingKey);
            consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(final String consumerTag, final Envelope envelope, final BasicProperties properties, final byte[] body) throws IOException {
                    executorService.submit(() -> parseAndDeliverToAtmosphere(body));
                }
            };
        } catch (Exception e) {
            String msg = "Unable to configure RabbitMQBroadcaster";
            logger.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    synchronized static RabbitMQConnectionFactory getFactory(AtmosphereConfig config) {
        // No need to synchronize here as the first Broadcaster created is at startup.
        if (factory == null) {
            factory = new RabbitMQConnectionFactory(config);
        }
        return factory;
    }

    private void parseAndDeliverToAtmosphere(final byte[] body) {
        try {
            Entry<byte[], List<Id>> e = parse(body);
            deliverToAtmosphere(e.getKey(), e.getValue());
        } catch (IOException e) {
            String msg = "Unable to parse body of incoming message";
            logger.error(msg, e);
        }
    }

    private Entry<byte[], List<Id>> parse(final byte[] body) throws IOException {
        Object decoded = bdecoder.decode(body);
        if (!(decoded instanceof SortedMap)) {
            throw new IOException("Unexpected " + decoded + ". Expected bencoded map");
        }
        SortedMap decodedMap = (SortedMap) decoded;
        Object dest = decodedMap.get("dest");
        if (!(dest instanceof List)) {
            throw new IOException("Unexpected " + dest + ". Expected bencoded list as '" + KEY_DESTINATION + "'");
        }
        Object message = ((SortedMap) decoded).get(KEY_MESSAGE);
        if (!(message instanceof byte[])) {
            throw new IOException("Unexpected type " + message + ". Expected bencoded string as '" + KEY_MESSAGE + "'");
        }

        ArrayList<Id> destinations = new ArrayList<>();
        for (Object o : ((List) dest)) {
            if (!(o instanceof String)) {
                throw new IOException("Unexpected type " + o + ". Expected bencoded string as '" + KEY_MESSAGE + "'");
            }
            destinations.add(new Id(o.toString()));
        }
        return new SimpleEntry<>((byte[]) message, destinations);
    }

    private void deliverToAtmosphere(final byte[] message, final List<Id> destinations) {
        for (Id destination : destinations) {
            for (MessageConsumer messageConsumer : listeners.getOrDefault(destination, emptyList())) {
                messageConsumer.consumeMessage(message);
            }
        }
    }

    @Override
    public void shutdown() {
        try {
            channel.close();
            connection.close();
        } catch (IOException | TimeoutException e) {
            logger.trace("", e);
        }
    }

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        logger.info("Recieved shutdownCompleted", cause);
    }

    public synchronized void deregisterListener(final Id id, final MessageConsumer messageConsumer) {
        List<MessageConsumer> messageConsumers = listeners.computeIfAbsent(id, (x) -> new ArrayList<>());
        // Trick to avoid having to synchronize accesses to the list
        List<MessageConsumer> copy = new ArrayList<>(messageConsumers);
        copy.remove(messageConsumer);
        listeners.put(id, copy);
    }

    public synchronized void registerListener(final Id id, final MessageConsumer messageConsumer) {
        List<MessageConsumer> messageConsumers = listeners.computeIfAbsent(id, (x) -> new ArrayList<>());
        // Trick to avoid having to synchronize accesses to the list
        List<MessageConsumer> copy = new ArrayList<>(messageConsumers);
        copy.add(messageConsumer);
        listeners.put(id, copy);
    }

    public void push(final Deliver entry, Id id) throws IOException {
        TreeMap<Object, Object> message = new TreeMap<>();
        message.put(KEY_SOURCE, id.getId());
        message.put(KEY_MESSAGE, entry.getMessage().toString().getBytes(StandardCharsets.UTF_8));
        byte[] bencoded = bencoder.encode(message);

        channel.basicPublish(EXCHANGE_NAME, outgoingRoutingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, bencoded);
    }
}
