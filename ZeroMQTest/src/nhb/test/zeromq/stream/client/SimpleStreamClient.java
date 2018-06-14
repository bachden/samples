package nhb.test.zeromq.stream.client;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.text.DecimalFormat;

import org.zeromq.ZMQ;

import com.nhb.common.utils.TimeWatcher;
import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.ZeroMQTest;

public class SimpleStreamClient extends ZeroMQTest {

	static final CharsetEncoder encoder = Charset.defaultCharset().newEncoder();

	public static void main(String[] args) {
		new SimpleStreamClient().runTest();
	}

	@Override
	protected void test() throws Exception {
		final int total = (int) 1;
		int msgSize = 1024;
		DecimalFormat df = new DecimalFormat("###,###.##");
		final TimeWatcher timeWatcher = new TimeWatcher();

		double totalDataSize = Double.valueOf(msgSize + 4) * total; // 4 for reversed int length value
		double dataMB = totalDataSize / (1024 * 1024);
		double dataGB = dataMB / 1024;

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < msgSize - 2; i++) {
			sb.append(i % 10);
		}
		sb.append("\n");
		String msgStr = sb.toString();

		System.out.println("Message: " + msgStr);
		System.out.println("Message size: " + df.format(msgSize) + " Bytes"
				+ (msgSize >= 1024
						? (" == " + (msgSize > 1024 * 1024 ? (df.format(Double.valueOf(msgSize) / 1024 / 1024) + "MB")
								: (df.format(Double.valueOf(msgSize) / 1024) + "KB")))
						: ""));
		System.out.println("Number of messages: " + df.format(total));
		System.out.println("Total size: " + df.format(dataMB) + "MB"
				+ (dataMB >= 1024 ? (" == " + df.format(dataGB) + "GB") : ""));

		// ByteBuffer length = ByteBuffer.allocate(4);
		// ByteBuffer body = ByteBuffer.allocateDirect(msgSize);
		// String str = sb.toString();

		byte[] msgBytes = msgStr.getBytes();
		final ZMQSocket socket = this.openSocket("tcp://localhost:8787", ZMQSocketType.STREAM_CONNECT);

		byte[] myId = new byte[5];
		socket.recv(myId, 0, 5, 0);
		socket.recv();

		Thread.sleep(100);

		int count = total;
		timeWatcher.reset();
		while (count-- > 0) {
			// length.clear();
			// length.putInt(str.length());
			// byte[] lengthBytes = length.array();
			// socket.send(lengthBytes, ZMQ.SNDMORE);
			// System.out.println("Sending " + msgStr);
			socket.send(myId, ZMQ.SNDMORE);
			socket.send(msgBytes, 0);
		}

		long timeNano = timeWatcher.endLapNano();
		double timeMillis = Double.valueOf(timeNano) / 1e6;

		System.out.println("*********** DONE ***********");
		System.out.println("Elasped time: " + df.format(timeMillis) + " milliseconds"
				+ (timeMillis > 1000 ? (" == " + df.format(timeMillis / 1000) + " seconds") : ""));
		System.out.println("Latency for 1 message: " + df.format(Double.valueOf(timeNano) / total) + " nanoseconds"
				+ (timeNano < 1000 ? ""
						: (" == " + df.format(Double.valueOf(timeNano / 1e3) / total) + " microseconds") //
								+ (timeNano < (int) 1e6 ? ""
										: (" == " + df.format(Double.valueOf(timeNano / 1e6) / total)
												+ " milliseconds"))));
		System.out.println("Throughput: " + (df.format(dataMB * 1e9 / timeNano) + " MB/s == ")
				+ (df.format(dataGB * 1e9 / timeNano) + " GB/s"));
		System.out.println("TPS: " + df.format(Double.valueOf(total) * 1e9 / timeNano));

		Thread.sleep(500l);

		// socket.close();
		// System.exit(0);
	}
}