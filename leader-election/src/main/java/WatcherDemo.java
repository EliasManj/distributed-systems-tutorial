import lombok.Getter;
import lombok.Setter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

@Getter
@Setter
public class WatcherDemo implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String TARGET_ZNONE = "/target_znode";
    private ZooKeeper zooKeeper;


    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void watchTargetZnode() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(TARGET_ZNONE, this);
        if (stat == null){
            return;
        }
        byte[] data = zooKeeper.getData(TARGET_ZNONE, this, stat);
        List<String> children = zooKeeper.getChildren(TARGET_ZNONE, this);
        System.out.println("Data : " + new String(data) + ", children : " + children);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch(watchedEvent.getType()){
            case None:
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper){
                        System.out.println("Disconnected from zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
            case NodeDeleted:
                System.out.println(TARGET_ZNONE + " was deleted");
                break;
            case NodeCreated:
                System.out.println(TARGET_ZNONE + " was created");
                break;
            case NodeDataChanged:
                System.out.println(TARGET_ZNONE + " data changed");
                break;
            case NodeChildrenChanged:
                System.out.println(TARGET_ZNONE + " children changed");
                break;
        }
        try {
            watchTargetZnode();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
