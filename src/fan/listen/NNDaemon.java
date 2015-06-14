package fan.listen;

import org.apache.zookeeper.KeeperException;

import fan.zk.TxnState;
import fan.zk.Zookeeper4Hdfs;

public class NNDaemon implements Runnable{
	Zookeeper4Hdfs zkForHdfs;
	TxnState state = null;
	boolean locked = false;
	long nnId = 1;
	
	public void run(){
		try{
			while(true){
				state = zkForHdfs.getTxnIdFileState();
				switch(state.text){
				case "PREPARE_LOCK":
					//TODO
					if(!locked){
						locked = true;
						zkForHdfs.createReplyId(nnId);
					}
				case "RELEASE_LOCK":
					//TODO 
				}
			}
		}catch(InterruptedException e){
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
