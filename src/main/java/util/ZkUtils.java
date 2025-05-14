package util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZkUtils {
    private static final String ZK_ADDRESS = "localhost:2181";  // ZooKeeper 默认的服务地址和端口
    private static final int SESSION_TIMEOUT = 15000;

    public static CuratorFramework createZkClient() {
        //获得并返回一个zookeeper的客户端对象
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(ZK_ADDRESS)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        client.start();
        return client;
    }
}