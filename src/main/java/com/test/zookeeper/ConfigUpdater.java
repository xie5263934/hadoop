package com.test.zookeeper;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ConfigUpdater {
    public static final String PATH = "/config";
    private ActiveKeyValueStore store;
    private Random random = new Random();

    public ConfigUpdater(String hosts) throws IOException {
        store = new ActiveKeyValueStore();
        store.connect(hosts);
    }

    public void run() throws KeeperException, InterruptedException {
        while(true){
            String value = random.nextInt(100)+"";
            store.write(PATH,value);
            System.out.printf("Set %s to %s\n",PATH,value);
            TimeUnit.SECONDS.sleep(random.nextInt(10));
        }
    }

    public static void main(String [] args) throws IOException, KeeperException, InterruptedException {
        ConfigUpdater configUpdater = new ConfigUpdater("master:2181");
        configUpdater.run();
    }
}
