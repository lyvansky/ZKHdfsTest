package fan.listen;

import java.io.IOException;

public class Client {
	public static void main(String[] args){
		TCPClient client = new TCPClient();
		byte[] data = "mv Fan Lee".getBytes();
		String opType = new String(data).split(" ")[0].trim();
		if (TCPClient.isTxnOperation(opType)) {
			try {
				TCPClient.processByCoordinator(opType, data);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			TCPClient.doLocalOperation();
		}
	}
}
	