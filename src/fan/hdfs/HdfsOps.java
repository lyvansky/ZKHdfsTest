package fan.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import fan.zk.TxnState;
import fan.zk.Zookeeper4Hdfs;

public class HdfsOps{

	//FSNamesystem namesystem;
	// NameNodeRpcServer rpc;
	//FileSystem fs;
	private static Zookeeper4Hdfs zkForHdfs = new Zookeeper4Hdfs();
	private static boolean hasReply = false;
	private static boolean hasReleaseLock = false;
	public static final long nnId = 1;
	/*
	public void connect(String host) throws IOException, InterruptedException{
		zkForHdfs.initZooKeeper(host);;
	}
	*/
	private static short mutex = 0;
	//public static CheckFile chk;
	
	public void lock() throws InterruptedException {
		System.out.println("Locking...");
		Thread.sleep(2000);
		System.out.println("Locked!");

	}

	public void unlock() throws InterruptedException{
		System.out.println("unlock...");	
		Thread.sleep(2000);
		System.out.println("Unlocked!");	
		
	}
	
	public void delete() {
		// TODO delete operation
	}
	
	public void doRename(){
		//TODO actually rename operation
		System.out.println("Do Rename...");
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Renamed!");
	}
	

	/**
	 * 递归的获取指定目录的所有子节点
	 * 
	 * @param path
	 * @return　
	 * @throws KeeperException 
	 * @throws InterruptedException 
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	/*
	public List<String> getFullPathRecursively(String path)
			throws FileNotFoundException, IllegalArgumentException, IOException {

		FileStatus tmp = null;
		String tmpDir = path, tmpFile = path;
		List<String> allPath = new ArrayList<String>();

		FileStatus file = fs.getFileStatus(new Path(path));
		if (file.isFile()) {
			allPath.add(path);
		} else {
			FileStatus fileList[] = fs.listStatus(new Path(path));
			for (int i = 0; i < fileList.length; i++) {
				tmp = fileList[i];
				if (tmp.isDirectory()) {
					tmpDir += "/" + tmp.getPath().getName();
					allPath.add(tmpDir);
					allPath.addAll(getFullPathRecursively(tmpDir));
				} else {
					tmpFile += "/" + tmp.getPath().getName();
					allPath.add(tmpFile);
				}
			}
		}
		return allPath;
	}
*/
	public void rename(String src, String dst) throws InterruptedException, KeeperException {
		// TODO rename operation
		boolean flag = false;
		String ops = "RENAME:" + src + "-->" + dst;
		System.out.println(ops);
		flag = getZkForHdfs().prepareLock(ops);
		//System.out.println("flag:" + flag);
		//System.out.println("hahah");
		if(flag){
			int i = 0;
			lock();
			//System.out.println("hasReceivedAllNNReply" + zkForHdfs.hasReceivedAllNNReply());
			//System.out.println("enter while loop....");
			while(!getZkForHdfs().hasReceivedAllNNReply()){
				Thread.sleep(1000);
				System.out.println("waitting for receivingall NN Reply..." + i + "s!");
			}
			doRename();;
			getZkForHdfs().updateTxnIdFileState(TxnState.RELEASE_LOCK);
			Thread.sleep(10000);
			getZkForHdfs().deleteTxnIdFile();
		}
	}

	public static Zookeeper4Hdfs getZkForHdfs() {
		return zkForHdfs;
	}

	public static void setZkForHdfs(Zookeeper4Hdfs zkForHdfs) {
		HdfsOps.zkForHdfs = zkForHdfs;
	}
}
