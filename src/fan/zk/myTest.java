package fan.zk;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;

public class myTest {
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		
		ZooKeeper zk = 
				new ZooKeeper("localhost:2181", ClientBase.CONNECTION_TIMEOUT, new Watcher() {
					// 监控所有被触发的事件
					public void process(WatchedEvent event) {
						System.out.println("已经触发了" + event.getType() + "事件！");
					}
				});
		System.out.println("nihaoma");
		// 创建一个目录节点
		//zk.create(path, data, acl, createMode)
		//zk.delete(path, version);
		zk.create("/eclipseRootPath", "eclipseRootPath".getBytes(),
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		// 创建一个子目录节点
		zk.create("/eclipseRootPath/testChildPathOne",
				"testChildDataOne".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out.println(new String(zk.getData("/eclipseRootPath", false, null)));
		// 取出子目录节点列表
		System.out.println(zk.getChildren("/eclipseRootPath", true));
		// 修改子目录节点数据
		zk.setData("/eclipseRootPath/testChildPathOne",
				"modifyChildDataOne".getBytes(), -1);
		System.out.println("目录节点状态：[" + zk.exists("/eclipseRootPath", true) + "]");
		// 创建另外一个子目录节点
		zk.create("/eclipseRootPath/testChildPathTwo",
				"testChildDataTwo".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out.println(new String(zk.getData(
				"/eclipseRootPath/testChildPathTwo", true, null)));
		List<String> children = zk.getChildren("/eclipseRootPath", null);
		for(Iterator<String> i = children.iterator(); i.hasNext();){
			String child = i.next();
			System.out.println("child:" + child);
			
		}
		// 删除子目录节点
		//zk.delete("/testRootPath/testChildPathTwo", -1);
		//zk.delete("/testRootPath/testChildPathOne", -1);
		// 删除父目录节点
		//zk.delete("/testRootPath", -1);
		// 关闭连接
		zk.close();
	}
}
