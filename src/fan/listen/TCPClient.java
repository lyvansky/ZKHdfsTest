package fan.listen;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

import fan.hdfs.HdfsOps;
import fan.zk.TxnState;
import fan.zk.Zookeeper4Hdfs;

public class TCPClient implements Runnable {

	private static final String server = "localhost";
	private static final int servPort = 2281;

	private boolean hasReply = false;
	private boolean hasReleaseLock = false;

	private static final long FILE_CHK_INTERVAL = 1000;

	HdfsOps hdfsOps = TCPServer.ops;
	Zookeeper4Hdfs zkForHdfs = new Zookeeper4Hdfs();
	TxnState state = null;

	public static final long nnId = 1;

	public static void main(String[] args) throws KeeperException,
			InterruptedException {
		// byte[] by = bytee.;
		// TxnState stat = zkForHdfs.getTxnIdFileState();
		Thread daemon = new Thread(new TCPClient());
		daemon.setDaemon(false);// 设置为后台线程
		daemon.start();

		// TimeUnit.MILLISECONDS.sleep(10000);
		// System.out.println("hasReply:" + hasReply);
	}

	public static void processByCoordinator(String opType, byte[] data)
			throws UnknownHostException, IOException {

		Socket socket = new Socket(server, servPort);

		switch (opType.toLowerCase()) {
		case "mv":
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();

			out.write(data);
			int totalBytesRcvd = 0;
			int bytesRcvd;

			while (totalBytesRcvd < data.length) {
				if ((bytesRcvd = in.read(data, totalBytesRcvd, data.length
						- totalBytesRcvd)) == -1) {
					throw new SocketException("Connection closed prematurely");
				}
				totalBytesRcvd += bytesRcvd;
			}
			socket.close();
		}
	}

	public static void doLocalOperation() {
	}

	public static boolean isTxnOperation(String type) {
		boolean ret = true;
		// TODO
		return ret;
	}

	public void printUsage() {
		System.out.println("mv [src] [dst]");
	}

	public void run() {
		while (true) {
			// System.out.println("后台程序...");
			try {
				if (zkForHdfs.isExists()) {
					try {
						//System.out.println("isExits:" + zkForHdfs.isExists());
						state = zkForHdfs.getTxnIdFileState();
					} catch (KeeperException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					switch (state.text) {
					case "PREPARE_LOCK":
						// TODO
						//System.out.println("hasReply:" + hasReply);
						if (!hasReply) {
							// locked = true;
							try {
								System.out.println("1、hasReply:" + hasReply);
								hasReply = zkForHdfs.createReplyId(nnId);
								System.out.println("2、hasReply:" + hasReply);
							} catch (KeeperException | InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					case "RELEASE_LOCK":
						// TODO
						if (!hasReleaseLock) {
							hasReleaseLock = zkForHdfs.deleteReplyFile(nnId);
						}
					}
					try {
						TimeUnit.MILLISECONDS.sleep(FILE_CHK_INTERVAL);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
