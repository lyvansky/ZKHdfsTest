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
	static Zookeeper4Hdfs zkForHdfs = new Zookeeper4Hdfs();
	private static boolean hasReply = false;
	private static boolean hasReleaseLock = false;
	private static long nnId = 1;
	/*
	public void connect(String host) throws IOException, InterruptedException{
		zkForHdfs.initZooKeeper(host);;
	}
	*/
	private static short mutex = 0;
	//public static CheckFile chk;

	public static Thread listen = new Thread(){
		public void run() {
			// TODO Auto-generated method stub
			TxnState stat = null;
			try {
				stat = zkForHdfs.getTxnIdFileState();
				System.out.println("stat.text:" + stat.text);
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			switch(stat.text){
			case "PREPARE_LOCK":
				if(!hasReply){
					try {
						hasReply = zkForHdfs.createReplyId(nnId);
					} catch (KeeperException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				break;
			case "RELEASE_LOCK":
				if(!hasReleaseLock){
					hasReleaseLock = zkForHdfs.deleteReplyFile(nnId);
				}
			}
		}
	};
	
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
		flag = zkForHdfs.prepareLock(ops);
		//System.out.println("flag:" + flag);
		//System.out.println("hahah");
		if(flag){
			int i = 0;
			lock();
			//System.out.println("hasReceivedAllNNReply" + zkForHdfs.hasReceivedAllNNReply());
			//System.out.println("enter while loop....");
			while(!zkForHdfs.hasReceivedAllNNReply()){
				Thread.sleep(1000);
				System.out.println("waitting for receivingall NN Reply..." + i + "s!");
			}
			doRename();;
			zkForHdfs.updateTxnIdFileState(TxnState.RELEASE_LOCK);
			Thread.sleep(10000);
			zkForHdfs.deleteTxnIdFile();
		}
	}
}
