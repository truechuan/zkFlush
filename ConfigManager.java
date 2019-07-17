import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

/**
 * @Author chenchuan
 * @Date 2019/7/16 4:00 PM
 */
@Service
@Slf4j
public class ConfigManager implements InitializingBean {

    private static final String DEFAULT_DATE = "config";

    @Autowired
    private ApplicationContext appContext;

    @Autowired
    private BaseZookeeper baseZookeeper;

    @Setter
    @Value("${zk.prefix}")
    private String prefix;

    private void registerConfigChangeListener(String configNode, IConfService confService) {
        String frefixPath = prefix + configNode;
        log.info("regist config start.[{}]", frefixPath);
        ZooKeeper zooKeeper = baseZookeeper.getZookeeper();
        try {
            if (null == zooKeeper.exists(frefixPath, null)) {
                baseZookeeper.createMultiNode(frefixPath, DEFAULT_DATE);
            }
            byte[] newConfig = zooKeeper.getData(frefixPath, event -> loadDat(confService), null);
            log.info("regist config success.[{}][{}]", frefixPath, new String(newConfig));
        } catch (Exception e) {
            log.error("regist config error.[{}]", frefixPath, e);
            throw new RuntimeException(e);
        }
    }

    public void loadDat(IConfService confService) {
        confService.loadConf();
    }

    public void updateConfig(String configNode, String newConfig) throws PawnException {
        String frefixPath = prefix + configNode;
        try {
            baseZookeeper.getZookeeper().setData(frefixPath, newConfig.getBytes(), -1);
        } catch (InterruptedException | KeeperException e) {
            log.error("updateConfig error.[{}]", configNode);
            throw PawnException.Builder.begin()
                    .withErroCodes(ErrorCodes.ConfigUpdateError, frefixPath)
                    .build();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        appContext.getBeansOfType(IConfService.class).forEach((name, bean) -> {
            String className = bean.getClass().getCanonicalName();
            registerConfigChangeListener("/" + className, bean);
        });
    }
}
