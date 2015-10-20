/**
* Based on code by Pavlo Baron, Landro Silva & Ryan Tenney
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
**/

package com.bol.log4j;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.SafeEncoder;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;



public class RedisAppender extends AppenderSkeleton implements Runnable, RedisAppenderMBean {

    // configs
    private String host = "localhost";
    private int port = 6379;
    private String password;
    private String key;
    private int queueSize = 5000;
    private int batchSize = 100;
    private long flushInterval = 500;
    private boolean alwaysBatch = true;
    private boolean purgeOnFailure = true;
    private long waitTerminate = 1000;
    private boolean registerMBean = true;

    // runtime stuff
    private Queue<LoggingEvent> events;
    private int messageIndex = 0;
    private byte[][] batch;
    private Jedis jedis;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> task;

    // metrics
    private int eventCounter = 0;
    private int eventsDroppedInQueueing = 0;
    private int eventsDroppedInPush = 0;
    private int connectCounter = 0;
    private int connectFailures = 0;
    private int batchPurges = 0;
    private int eventsPushed = 0;



    @Override
    public void activateOptions() {
        try {
            super.activateOptions();

            if (key == null) throw new IllegalStateException("Must set 'key'");
            if (!(flushInterval > 0)) throw new IllegalStateException("FlushInterval (ex. Period) must be > 0. Configured value: " + flushInterval);
            if (!(queueSize > 0)) throw new IllegalStateException("QueueSize must be > 0. Configured value: " + queueSize);
            if (!(batchSize > 0)) throw new IllegalStateException("BatchSize must be > 0. Configured value: " + batchSize);

            if (executor == null) executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getSimpleName(), true));

            if (task != null && !task.isDone()) task.cancel(true);

            events = new ArrayBlockingQueue<LoggingEvent>(queueSize);
            batch = new byte[batchSize][];
            messageIndex = 0;

            createJedis();

            if (registerMBean) {
                registerMBean();
            }

            task = executor.scheduleWithFixedDelay(this, flushInterval, flushInterval, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LogLog.error("Error during activateOptions", e);
        }
    }

    protected void createJedis() {
        if (jedis != null && jedis.isConnected()) {
            jedis.disconnect();
        }
        jedis = new Jedis(host, port);
    }

    @Override
    protected void append(LoggingEvent event) {
        try {
            eventCounter++ ;
            int size = events.size();
            if (size < queueSize) {
                populateEvent(event);
                try {
                    events.add(event);
                } catch (IllegalStateException e) {
                    // safeguard in case multiple threads raced on almost full queue
                    eventsDroppedInQueueing++;
                }
            } else {
                eventsDroppedInQueueing++;
            }

        } catch (Exception e) {
            errorHandler.error("Error populating event and adding to queue", e, ErrorCode.GENERIC_FAILURE, event);
        }
    }

    protected void populateEvent(LoggingEvent event) {
        event.getThreadName();
        event.getRenderedMessage();
        event.getNDC();
        event.getMDCCopy();
        event.getThrowableStrRep();
        event.getLocationInformation();
    }

    @Override
    public void close() {
        try {
            task.cancel(false);
            executor.shutdown();

            boolean finished = executor.awaitTermination(waitTerminate, TimeUnit.MILLISECONDS);
            if (finished) {
                // We finished successfully, process any events in the queue if any still remain.
                if (!events.isEmpty())
                    run();
                // Flush any remainder regardless of the alwaysBatch flag.
                if (messageIndex > 0)
                    push();
            } else {
                LogLog.warn("Executor did not complete in " + waitTerminate + " milliseconds. Log entries may be lost.");
            }

            safeDisconnect();
        } catch (Exception e) {
            errorHandler.error(e.getMessage(), e, ErrorCode.CLOSE_FAILURE);
        }
    }

    /**
     * Pre: jedis not null
     */
    protected void safeDisconnect() {
        try {
            jedis.disconnect();
        } catch (Exception e) {
            LogLog.warn("Disconnect failed to Redis at " + getRedisAddress());
        }
    }

    protected boolean connect() {
        try {
            if (!jedis.isConnected()) {
                LogLog.debug("Connecting to Redis at " + getRedisAddress());
                connectCounter++;
                jedis.connect();

                if (password != null) {
                    String result = jedis.auth(password);
                    if (!"OK".equals(result)) {
                        LogLog.error("Error authenticating with Redis at " + getRedisAddress());
                    }
                }

                // make sure we got a live connection
                jedis.ping();
            }
            return true;
        } catch (Exception e) {
            connectFailures++;
            LogLog.error("Error connecting to Redis at " + getRedisAddress() + ": " + e.getMessage());
            return false;
        }
    }

    public void run() {

        if (!connect()) {
            return;
        }

        try {
            if (messageIndex == batchSize) push();

            LoggingEvent event;
            while ((event = events.poll()) != null) {
                try {
                    String message = layout.format(event);
                    batch[messageIndex++] = SafeEncoder.encode(message);
                } catch (Exception e) {
                    errorHandler.error(e.getMessage(), e, ErrorCode.GENERIC_FAILURE, event);
                }

                if (messageIndex == batchSize) push();
            }

            if (!alwaysBatch && messageIndex > 0) {
                // push incomplete batches
                push();
            }

        } catch (JedisException je) {

            LogLog.debug("Can't push " + messageIndex + " events to Redis. Reconnecting for retry.", je);
            safeDisconnect();

        } catch (Exception e) {
            errorHandler.error("Can't push events to Redis", e, ErrorCode.GENERIC_FAILURE);
        }
    }

    protected boolean push() {
        LogLog.debug("Sending " + messageIndex + " log messages to Redis at " + getRedisAddress());
        try {

            jedis.rpush(SafeEncoder.encode(key),
                batchSize == messageIndex
                    ? batch
                    : Arrays.copyOf(batch, messageIndex));

            eventsPushed += messageIndex;
            messageIndex = 0;
            return true;

        } catch (JedisDataException jde) {
            // Handling stuff like OOM's on Redis' side
            if (purgeOnFailure) {
                errorHandler.error("Can't push events to Redis at " + getRedisAddress() + ": " + jde.getMessage());
                eventsDroppedInPush += messageIndex;
                batchPurges++;
                messageIndex = 0;
            }
            return false;
        }
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getRedisAddress() {
        return host + ":" + port;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setFlushInterval(long millis) {
        this.flushInterval = millis;
    }

    // deprecated, use flushInterval instead
    public void setPeriod(long millis) {
        setFlushInterval(millis);
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setPurgeOnFailure(boolean purgeOnFailure) {
        this.purgeOnFailure = purgeOnFailure;
    }

    public void setAlwaysBatch(boolean alwaysBatch) {
        this.alwaysBatch = alwaysBatch;
    }

    public void setRegisterMBean(boolean registerMBean) {
        this.registerMBean = registerMBean;
    }

    public void setWaitTerminate(long waitTerminate) {
        this.waitTerminate = waitTerminate;
    }

    public boolean requiresLayout() {
        return true;
    }

    public int getEventCounter() { return eventCounter; }
    public int getEventsDroppedInQueueing() { return eventsDroppedInQueueing; }
    public int getEventsDroppedInPush() { return eventsDroppedInPush; }
    public int getConnectCounter() { return connectCounter; }
    public int getConnectFailures() { return connectFailures; }
    public int getBatchPurges() { return batchPurges; }
    public int getEventsPushed() { return eventsPushed; }
    public int getEventQueueSize() { return events.size(); }

    private void registerMBean() {
        Class me = this.getClass();
        String name = me.getPackage().getName() + ":type=" + me.getSimpleName();

        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName obj_name = new ObjectName(name);
            RedisAppenderMBean mbean = this;
            mbs.registerMBean(mbean, obj_name);
        } catch (Exception e) {
            LogLog.error("Unable to register mbean", e);
        }

        LogLog.warn("INFO: Registered MBean " + name);

    }

}
