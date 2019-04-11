package com.test.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;

public class ConfigWatcher implements Watcher {

    private ActiveKeyValueStore store;

    public ConfigWatcher(String hosts) throws IOException {
        store = new ActiveKeyValueStore();
        store.connect(hosts);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
            try {
                displayConfig();
            } catch (KeeperException e) {
                System.out.printf("KeeperException:%s.Exiting.\n", e);
            } catch (InterruptedException e) {
                System.out.println("interrupted exiting");
                Thread.currentThread().interrupt();
            }
        }
    }

    public void displayConfig() throws KeeperException, InterruptedException {
        String value = store.read(ConfigUpdater.PATH, this);
        System.out.printf("Read %s as %s \n", ConfigUpdater.PATH, value);
    }

    public static void main(String [] args) throws IOException, KeeperException, InterruptedException {
        ConfigWatcher configWatcher = new ConfigWatcher("master:2181");
        configWatcher.displayConfig();
        Thread.sleep(Long.MAX_VALUE);
    }
}
