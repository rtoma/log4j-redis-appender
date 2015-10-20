package com.bol.log4j;

public interface RedisAppenderMBean {

    public int getEventCounter();
    public int getEventsDroppedInQueueing();
    public int getEventsDroppedInPush();
    public int getConnectCounter();
    public int getConnectFailures();
    public int getBatchPurges();
    public int getEventsPushed();
    public int getEventQueueSize();

}
