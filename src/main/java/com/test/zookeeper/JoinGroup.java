package com.test.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;

public class JoinGroup extends ConnectionWatcher{

    public void join(String groupName,String memberName) throws KeeperException, InterruptedException {
        String path = "/"+groupName+"/"+memberName;
        String createPath = zk.create(path,null, ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Created "+createPath);
    }

    public static void main(String [] args) throws IOException, KeeperException, InterruptedException {
        JoinGroup joinGroup = new JoinGroup();
        joinGroup.connect("master:2181");
        joinGroup.join("root","sheldon");
        joinGroup.connectedSignal.await();
        System.out.println("joinGroup successfully");
    }
}
