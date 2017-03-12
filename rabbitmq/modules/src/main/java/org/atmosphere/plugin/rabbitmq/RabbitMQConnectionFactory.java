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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import org.atmosphere.cpr.AtmosphereConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQ Connection Factory.
 *
 * @author Thibault Normand
 * @author Jean-Francois Arcand
 */
public class RabbitMQConnectionFactory implements AtmosphereConfig.ShutdownHook, ShutdownListener {
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

    public RabbitMQConnectionFactory(AtmosphereConfig config) {

        String exchange = config.getInitParameter(PARAM_EXCHANGE_TYPE, "topic");

        boolean useSsl = config.getInitParameter(PARAM_USE_SSL, false);



        exchangeName = "atmosphere." + exchange;
        config.shutdownHook(this);

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
            channel.addShutdownListener(this);

            logger.debug("Topic creation '{}'...", exchangeName);
            channel.exchangeDeclare(exchangeName, exchange);
        } catch (Exception e) {
            String msg = "Unable to configure RabbitMQBroadcaster";
            logger.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    public final static RabbitMQConnectionFactory getFactory(AtmosphereConfig config) {
        // No need to synchronize here as the first Broadcaster created is at startup.
        if (factory == null) {
            factory = new RabbitMQConnectionFactory(config);
        }
        return factory;
    }

    public String exchangeName() {
        return exchangeName;
    }

    public Channel channel() {
        return channel;
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
}
