package fan.hdfs.test;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import fan.hdfs.HdfsOps;
import fan.hdfs.StateManager;
import fan.zk.Zookeeper4Hdfs;

public class HdfsTest {
	//HdfsOps ops;
	//Zookeeper4Hdfs zkForHdfs;
	
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException{
		HdfsOps ops = new HdfsOps();
		//System.out.println("Connecting...");
		//ops.connect("localhost:2181");
		//System.out.println("Connected!");
		//System.out.println("FAN LEE...!");
		ops.rename("Lee", "Fan");
		//System.out.println("Renamed!");
	}
}
