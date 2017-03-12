/*
 * Copyright 2017 Jean-Francois Arcand
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

import org.atmosphere.cpr.AtmosphereConfig;
import org.atmosphere.cpr.Broadcaster;
import org.atmosphere.cpr.BroadcasterFuture;
import org.atmosphere.cpr.Deliver;
import org.atmosphere.util.SimpleBroadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Less simple {@link org.atmosphere.cpr.Broadcaster} implementation based on RabbitMQ
 *
 * @author Thibault Normand
 * @author Jean-Francois Arcand
 */
public class RabbitMQNGBroadcaster extends SimpleBroadcaster implements MessageConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQNGBroadcaster.class);

    private Id oldKey;
    private RabbitMQConnectionFactory factory;

    public RabbitMQNGBroadcaster() {
    }

    @Override
    public Broadcaster initialize(String id, AtmosphereConfig config) {
        super.initialize(id, config);
        init(config);
        return this;
    }

    public void init(AtmosphereConfig config) {
        factory = RabbitMQConnectionFactory.getFactory(config);

        restartConsumer();
    }

    @Override
    public Broadcaster initialize(String name, java.net.URI uri, AtmosphereConfig config) {
        super.initialize(name, uri, config);
        init(config);
        return this;
    }

    @Override
    public void setID(String id) {
        super.setID(id);
        restartConsumer();
    }

    @Override
    public String getID() {
        String id = super.getID();
        if (id.startsWith("/*")) {
            id = "atmosphere";
        }
        return id;
    }

    @Override
    protected void push(Deliver entry) {
        try {
            logger.trace("Outgoing broadcast : {}", entry.getMessage());
            Id id = new Id(getID());

            factory.push(entry, id);


        } catch (IOException e) {
            logger.warn("Failed to send message over RabbitMQ", e);
        }
    }

    private void restartConsumer() {
        try {
            if (oldKey != null) {
                logger.debug("Unregister {}", oldKey);
                factory.deregisterListener(oldKey, this);
            }

            final Id id = new Id(getID());
            logger.info("Registering for routing key {}", id);
            factory.registerListener(id, this);

        } catch (Throwable ex) {
            String msg = "Unable to initialize RabbitMQBroadcaster";
            logger.error(msg, ex);
            throw new IllegalStateException(msg, ex);
        }
    }

    @Override
    public synchronized void releaseExternalResources() {
        try {
            factory.deregisterListener(new Id(getID()), this);
        } catch (Exception ex) {
            logger.trace("", ex);
        }
    }

    @Override
    public void consumeMessage(final byte[] body) {
        String message = new String(body, StandardCharsets.UTF_8);
        try {
            Object newMsg = filter(message);
            // if newSgw == null, that means the message has been filtered.
            if (newMsg != null) {
                deliverPush(new Deliver(newMsg, new BroadcasterFuture<>(newMsg), message), true);
            }
        } catch (Throwable t) {
            logger.error("failed to push message: " + message, t);
        }
    }
}