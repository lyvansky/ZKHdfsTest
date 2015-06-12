package fan.hdfs;

import org.apache.zookeeper.KeeperException;

import fan.zk.TxnState;
import fan.zk.Zookeeper4Hdfs;

/**
 * This class is used to detect the modify of the txn_id file periodically
 * 
 * @author lee
 *
 */
public class StateManager implements Runnable {

	public Zookeeper4Hdfs zkForHdfs;
	public HdfsOps ops;
	private static long nnId;//the id number of NameNode, global uniquely
	/**
	 * 根据检测到的事务文件的状态变化，进行删除、加锁等操作
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	@SuppressWarnings("unused")
	private void processTxn() throws KeeperException, InterruptedException {
		TxnState state = zkForHdfs.getTxnIdFileState();
		switch (state) {
		case PREPARE_LOCK:
			ops.lock();
			break;
		case RELEASE_LOCK:
			ops.unlock();
			zkForHdfs.delete();
			break;
		default:
			break;
		}
	}

	public void listen() throws KeeperException, InterruptedException {
		TxnState state = zkForHdfs.getTxnIdFileState();
		switch (state) {
		case PREPARE_LOCK:
			ops.lock();
			break;
		case RELEASE_LOCK:
			ops.unlock();
			zkForHdfs.deleteReplyFile(nnId);
			break;
		default:
			break;
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		// zkForHdfs.zk;
		try {
			listen();
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
