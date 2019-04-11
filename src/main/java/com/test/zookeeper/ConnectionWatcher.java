package com.test.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ConnectionWatcher implements Watcher {
    private static final int SESSION_TIMEOUT = 5000;

    protected ZooKeeper zk;
    protected CountDownLatch connectedSignal = new CountDownLatch(1);

    public void connect(String hosts) throws IOException {
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }
    }


    public void close() throws InterruptedException {
        zk.close();
    }
}
