import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Author chenchuan
 * @Date 2019/7/16 4:10 PM
 */
@Service
public class BaseZookeeper implements Watcher {
    @Getter
    private ZooKeeper zookeeper;

    @Value("${zk.url}")
    private String zkUrl;

    /**
     * 超时时间
     *     
     */
    private static final int SESSION_TIME_OUT = 2000;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == KeeperState.SyncConnected) {
            System.out.println("Watch received event");
            countDownLatch.countDown();
        }
    }

    /**
     * 连接zookeeper  
     */
    @PostConstruct
    public void connectZookeeper() throws Exception {
        zookeeper = new ZooKeeper(zkUrl, SESSION_TIME_OUT, this);
        countDownLatch.await();
        System.out.println("zookeeper connection success");
    }

    /**
     * 创建节点
     *    
     */
    public String createNode(String path, String data) throws Exception {
        return this.zookeeper.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void createMultiNode(String path, String data) throws Exception {
        String[] arr = path.split("/");
        List<String> list = Lists.newArrayList();
        for (String str : arr) {
            if (StringUtils.isNotBlank(str)) {
                list.add("/" + str);
                String tempPath = Joiner.on("").join(list);
                if (this.zookeeper.exists(tempPath, false) == null) {
                    createNode(tempPath, data);
                }
            }
        }
    }

    /**
     * 获取路径下所有子节点
     *    
     */
    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        List<String> children = zookeeper.getChildren(path, false);
        return children;
    }

    /**
     * 获取节点上面的数据
     *    
     */
    public String getData(String path) throws KeeperException, InterruptedException {
        byte[] data = zookeeper.getData(path, false, null);
        if (data == null) {
            return "";
        }
        return new String(data);
    }

    /**
     * 设置节点信息
     *    
     */
    public Stat setData(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zookeeper.setData(path, data.getBytes(), -1);
        return stat;
    }

    /**
     * 删除节点
     *    
     */
    public void deleteNode(String path) throws InterruptedException, KeeperException {
        zookeeper.delete(path, -1);
    }

    /**
     * 获取创建时间
     *    
     */
    public String getCTime(String path) throws KeeperException, InterruptedException {
        Stat stat = zookeeper.exists(path, false);
        return String.valueOf(stat.getCtime());
    }

    /**
     * 获取某个路径下孩子的数量
     *    
     */
    public Integer getChildrenNum(String path) throws KeeperException, InterruptedException {
        int childenNum = zookeeper.getChildren(path, false).size();
        return childenNum;
    }

    /**
     * 关闭连接
     *    
     */
    @PreDestroy
    public void closeConnection() throws InterruptedException {
        if (zookeeper != null) {
            zookeeper.close();
        }
    }
}
