package fan.zk;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.test.ClientBase;

public class Zookeeper4Hdfs implements Runnable {

	private static final String STATE_OPS_LOG_SEPARATOR = "@";
	private static String address = "172.16.253.178:2181";
	private String host;

	private static Map<String, String> hostMap = new HashMap<String, String>();// machine
																				// id-->ip
																				// address
	private static Map<Long, String> txnMap = new TreeMap<Long, String>();// 事务等待队列，按事务编号有序排列；记录事务编号-->事务文件(txn_id)的映射关系
	static {
		hostMap.put("5", "172.16.253.178");
		txnMap.put((long) 1, "txn_1");
		// txnMap.put((long) 2, "txn_2");
	}
	// String currentTxnIdFile;
	private static final String txnRoot = "/txn";
	private static final String txnDirPrefix = "/txn/txn_";
	private static final String replyFilePrefix = "reply_";// reply_id文件前缀

	private static final long REPLY_FILE_CHK_INTERVAL = 1000;// 每秒钟检查一次是否所有NN都reply了

	private static volatile long txnIDNumber = 0;// 当前事务的编号
	private static volatile long waitTxn = 0; // 正在等待的事务数
	private static volatile long processingTxnId = 1;
	private static volatile String currentTxnIdFile = txnDirPrefix
			+ processingTxnId;// 当前正在执行的事务文件

	private static ZooKeeper zk;

	/*
	 * public void initZooKeeper(String host) throws IOException{ this.zk = new
	 * ZooKeeper(address, ClientBase.CONNECTION_TIMEOUT,new Watcher() { public
	 * void process(WatchedEvent event) {
	 * System.out.println("To do something..."); } }); }
	 */
	static {
		try {
			System.out.println("static...");
			zk = new ZooKeeper(address, ClientBase.CONNECTION_TIMEOUT,
					new Watcher() {
						public void process(WatchedEvent event) {
							System.out.println("To do something...");
						}
					});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 测试自定义监听
	public Watcher wh = new Watcher() {
		public void process(WatchedEvent event) {

			System.out.println("回调watcher实例： 路径" + event.getPath() + " 类型："
					+ event.getType());
		}
	};

	/**
	 * 本接口完成： <li>1. 创建目录/Txn/txn_id并设置状态为"prepare_lock"，一个txn_id对应一个事务</li> <li>
	 * 2. 创建文件/Txn/txn_id/tran，用于记录当前事务的具体操作</li>
	 * 
	 * @return true if create successfully, else false
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	// NN protocol, called by NN
	public boolean prepareLock(String ops) {
		// System.out.println("PrepareLock0...");
		// synchronized
		txnIDNumber++;
		waitTxn++;
		// processingTxnId = nextTxnID - waitTxn;
		String path = null, str;
		// String leader =
		// hostMap.keySet().iterator().next();//待续，目前是选择NN编号最小的NN为leader
		String state = TxnState.PREPARE_LOCK.text;// "prepare_lock";
		Stat stat = null;
		str = txnDirPrefix + processingTxnId; // str = "/Txn/txn_id"
		// data = state;// + STATE_OPS_LOG_SEPARATOR + ops;// 数据为状态和事务操作
		// synchronized (currentTxnIdFile){
		// if (txnIDNumber - waitTxn + 1 > processingTxnId) {
		currentTxnIdFile = str;
		// }
		// System.out.println("PrepareLock...str:" + str);
		try {
			if (zk.exists(txnRoot, false) == null) {
				zk.create(txnRoot, "txnRoot".getBytes(), Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			}
			stat = zk.exists(str, false);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (stat == null) {
			System.out.println("stat=null!");
			try {
				path = zk.create(str, state.getBytes(), Ids.OPEN_ACL_UNSAFE, // create
																				// file/dir
																				// "/Txn/txn_id
						CreateMode.PERSISTENT);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				// zk.delete(str, 0);
				e.printStackTrace();
			}
			// System.out.println("path:" + path + ", str:" + str);
			// System.out.println("path==str? " + path.equals(str));
			if (path.equals(str)) {
				System.out.println("Creating /txn/txn_id/tran....");
				str += "/tran"; // str = "/Txn/txn_id/tran"
				try {
					path = zk.create(str, ops.getBytes(), Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
				} catch (KeeperException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Tran Created!");
				return true;
				// System.out.println("path:" + path);
			} else {
				// zk.delete(path, version);
				// 第一步创建失败，则直接失败
				return false;
			}
		}
		System.out.println("path:" + path + ", str:" + str);
		return false;
	}

	/**
	 * 每个订阅了zookeeper的/Txn/txn_id/目录的NN，在收到变动
	 * 是否创建reply_id文件的判断由NN自行判断，因为上一个事务已经将NN的dir加锁了，因此直到之前的所有事务都执行完成后本次才可以加锁。
	 * 
	 * @param nnId
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public boolean createReplyId(long nnId) throws KeeperException,
			InterruptedException {
		// long id = getProcessingTxnId();
		String path = null, str = null;
		String state = "locked";

		// ZooKeeper zk = getZooKeeper();
		str = currentTxnIdFile + "/" + replyFilePrefix + nnId;
		System.out.println("str:" + str);
		Stat stat = zk.exists(str, false);
		// System.out.println("reply_id:" + str);
		if (stat == null) {
			System.out.println("Creating ReplyFile");
			path = zk.create(str, state.getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
		System.out.println("path:" + path);
		return str.equals(path);
	}

	/**
	 * 更新txn_id文件的状态信息
	 * 
	 * @param state
	 *            要更新的状态
	 * @return
	 * @return 如果更新成功则返回true，否则false
	 */
	public boolean updateTxnIdFileState(TxnState state) {
		boolean ret = false;
		String path = currentTxnIdFile;

		try {
			while (!hasReceivedAllNNReply()) {
				System.out.println("waitting all reply...");
			}
			// if (hasReceivedAllNNReply()) {
			System.out.println("Set the state of txn_id to \"Release_Lock\"");
			ret = (zk.setData(path, state.toString().getBytes(), -1) != null) ? true
					: false;
			System.out.println("Release_Lock!");
			// }
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret;
	}

	/**
	 * 更新reply_id文件的状态
	 * 
	 * @param state
	 *            要更新的状态
	 * @param nnId
	 *            NN的全局id号
	 * @return 如果更新成功则返回true，否则false
	 */
	public boolean updateReplyFileState(TxnState state, String nnId) {
		boolean ret = false;
		String path = currentTxnIdFile + "/" + replyFilePrefix + nnId;
		try {
			ret = (zk.setData(path, state.toString().getBytes(), -1) != null) ? true
					: false;
			// ret = (path == zk.setData(path, state.getBytes(), -1))?
			// true:false;
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret;
	}

	// Cannot switch on a value of only convertible int values
	public void processOps(TxnType type) {
		switch (type) {
		case RENAME:
			rename();
			break;
		case DELETE:
			delete();
			break;
		}
	}

	/**
	 * 嵌套的获取某个指定路径下的所有子节点
	 * 
	 * @return 如果成功则返回该路径下的所有子节点
	 */
	private List<String> getListing(String path) {
		List<String> list = null;
		// do something
		return list;
	}

	public void rename() {//
		// do something
	}

	public void delete() {
		// do something
		// waitTxn--;
	}

	public boolean deleteReplyFile(long nnId) {
		boolean ret = false;
		String path = currentTxnIdFile + "/" + replyFilePrefix + nnId;
		Stat stat = null;
		try {
			stat = zk.exists(path, false);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (stat != null) {
			try {
				zk.delete(path, -1);
				ret = true;
			} catch (InterruptedException | KeeperException e) {
				ret = false;
				e.printStackTrace();
			}
		}
		return ret;
	}

	long getNextTxnId() {// get the next txn number
		return txnIDNumber;
	}

	long getProcessingTxnId() {
		return txnIDNumber - waitTxn - 1;// 正在处理的txn编号
	}

	public boolean isExists() throws KeeperException, InterruptedException{
		boolean ret;
		return zk.exists(currentTxnIdFile, false) != null;
		//return ret;
	}
	public TxnState getTxnIdFileState() throws KeeperException,
			InterruptedException {
		String str;
		TxnState state = null;
		System.out.println("currentTxnIdFile:" + currentTxnIdFile);
		if (zk.exists(currentTxnIdFile, false) != null) {
			str = new String(zk.getData(currentTxnIdFile, false, null));
			//System.out.println("str:" + str);

			switch (str) {
			case "PREPARE_LOCK":
				state = TxnState.PREPARE_LOCK;
				break;
			case "EXEC_DELETE":
				state = TxnState.EXEC_DELETE;
				break;
			case "EXEC_RENAME":
				state = TxnState.EXEC_RENAME;
				break;
			case "RELEASE_LOCK":
				state = TxnState.RELEASE_LOCK;
				break;
			}
		}
		//System.out.println("state:" + state.text);
		return state;
	}

	/**
	 * 向指定coordinator发送事务，所有的事务操作均有coordinator来执行
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void sendTxnToCoordinator() throws KeeperException,
			InterruptedException {
		byte[] data = zk.getData(currentTxnIdFile, true, null);

		// TODO 向指定coordinator发送事务，之后该事务由coordinator来执行
	}

	/**
	 * 检查是否每个NN都成功reply了，即是否都成功创建了reply_id文件
	 * 如果没有部分NN没有reply，则等待REPLY_FILE_CHK_INTERVAL毫秒后再检查
	 * 
	 * @return 成功则返回true，否则false
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public boolean hasReceivedAllNNReply() throws InterruptedException,
			KeeperException {
		boolean ret = false;
		int i = 0;
		String path = currentTxnIdFile;
		// System.out.println("List...path:" + path);
		List<String> list = zk.getChildren(path, false);
		// System.out.println("while... list.size:" + list.size() +
		// ", hostMap.size:" + hostMap.size());
		do {
			// System.out.println("in whilesdfdfs...");
			if (list.size() == hostMap.size()) {// 每个NN都创建了reply_id文件
				// ret = true;
				// System.out.println("hasReceivedAllNNReply, while");
				return true;
			} else {
				System.out.println("Receiving all NN Reply..." + (++i));
				Thread.sleep(REPLY_FILE_CHK_INTERVAL);// 这层逻辑应该交由用户自己实现，目前采用等待1000ms后再次检查
				list = zk.getChildren(path, false);
			}
		} while (!ret);
		return ret;
	}

	public boolean isAllReplyFileDeleted() throws KeeperException,
			InterruptedException {
		boolean ret;
		String path = currentTxnIdFile;
		List<String> list = zk.getChildren(path, false);
		// System.out.println("list.size:" + list.size());
		ret = (list.size() == 1) ? true : false;// only one file in the
												// folder---tran
		return ret;
	}

	public static String getStateOpsLogSeparator() {
		return STATE_OPS_LOG_SEPARATOR;
	}

	/**
	 * 删除事务目录，并更新事务等待队列深度
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void deleteTxnIdFile() throws KeeperException, InterruptedException {
		System.out.println("deleting the TxnIdFile...");
		String path = currentTxnIdFile;
		String data = new String(zk.getData(path, false, null));
		// String stat = data.toString();
		boolean release = data.equals(TxnState.RELEASE_LOCK.toString());
		boolean rep = isAllReplyFileDeleted();
		System.out.println("data:" + data + ", State:"
				+ TxnState.RELEASE_LOCK.text);
		while ((!release) || (!rep)) {// waitting if no "RELEASE_LOCK" or
										// "NO Reply"
			// data = new String(zk.getData(path, false, null));
			release = data.equals(TxnState.RELEASE_LOCK.toString());
			rep = isAllReplyFileDeleted();
		}
		;
		// if (data == TxnState.RELEASE_LOCK.toString() &&
		// isAllReplyFileDeleted()) {
		List<String> children = zk.getChildren(path, false);
		for (Iterator<String> it = children.iterator(); it.hasNext();) {
			String child = it.next();
			zk.delete(path + "/" + child, -1);
		}
		zk.delete(path, -1);
		waitTxn--;
		processingTxnId++;
		System.out.println("TxnIdFile Deleted! This transaction is processed!");
	}

	public void run() {
		while (true) {
			try {
				deleteTxnIdFile();
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
