package fan.listen;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import org.apache.zookeeper.KeeperException;

import fan.hdfs.HdfsOps;
import fan.zk.Zookeeper4Hdfs;

public class TCPServer {
	private static final int BUFSIZE = 32;
	private boolean isCoordinator = false;

	static HdfsOps ops = new HdfsOps();
	//static Zookeeper4Hdfs zkForHdfs = new Zookeeper4Hdfs();
	
	public static void process(String[] args) throws InterruptedException, KeeperException{
		args[0] = args[0].trim();
		switch(args[0]){
		case "mv":
			doRename(args[1], args[2]);
			break;
		case "del":
			doDelete(args[1]);
			break;
		}
	}
	public static void doRename(String src, String dst) throws InterruptedException, KeeperException{
		src = src.trim();
		dst = dst.trim();
		
		ops.rename(src, dst);
	}
	
	public static void doDelete(String src){
		src = src.trim();
		
		ops.delete();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException{
		
		String ops;
		int servPort = 2281;
		ServerSocket servSock = new ServerSocket(servPort);
		int recvMsgSize;
		byte[] receiveBuf = new byte[BUFSIZE];
		
		while(true){
			Socket clntSock = servSock.accept();
			SocketAddress clientAddress = clntSock.getRemoteSocketAddress();
			InputStream in = clntSock.getInputStream();
			OutputStream out = clntSock.getOutputStream();
			
			while ((recvMsgSize = in.read(receiveBuf))!=-1){
				out.write(receiveBuf, 0, recvMsgSize);
			}
			
			ops = new String(receiveBuf);
			//ops = ops.trim();
			String[] str = ops.split(" ");
			System.out.println("str.length:" + str.length);
			process(str);
		}
	}
}
