package region;

import org.apache.curator.framework.CuratorFramework;
import util.ZkUtils;

import java.util.List;

/**
 * 清理 ZooKeeper 上残留的 /regions 子节点
 */
public class ZooKeeperCleanup {
    public static void clean(String parentPath) {
        try {
            CuratorFramework zk = ZkUtils.createZkClient();
            if (zk.checkExists().forPath(parentPath) != null) {
                List<String> children = zk.getChildren().forPath(parentPath);
                for (String child : children) {
                    String full = parentPath + "/" + child;
                    zk.delete().forPath(full);
                    System.out.println("[Cleanup] 删除旧节点 " + full);
                }
            }
            zk.close();
        } catch (Exception e) {
            System.err.println("[Cleanup] 清理 ZooKeeper 节点失败: " + e.getMessage());
        }
    }
}
