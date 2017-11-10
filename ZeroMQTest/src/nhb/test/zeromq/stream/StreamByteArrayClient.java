package nhb.test.zeromq.stream;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.text.DecimalFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.nhb.common.utils.TimeWatcher;
import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.ZeroMQTest;

public class StreamByteArrayClient extends ZeroMQTest {

	static final CharsetEncoder encoder = Charset.defaultCharset().newEncoder();

	public static void main(String[] args) {
		new StreamByteArrayClient().runTest();
	}

	@Override
	protected void test() throws Exception {
		final int total = (int) 1e6;
		int msgSize = 1024 * 4 - 4;
		DecimalFormat df = new DecimalFormat("###,###.##");
		final TimeWatcher timeWatcher = new TimeWatcher();
		final ZMQSocket socket = this.openSocket("tcp://localhost:8787", ZMQSocketType.STREAM_CONNECT);

		CountDownLatch connectedSignal = new CountDownLatch(1);
		AtomicBoolean connected = new AtomicBoolean(false);
		byte[] myId = new byte[5];

		Thread recvThread = new Thread(() -> {
			while (!Thread.currentThread().isInterrupted()) {
				try {
					socket.recv(myId, 0, 5, 0);
					if (connected.compareAndSet(false, true)) {
						connectedSignal.countDown();
					}
					socket.recv();
				} catch (ZMQException e) {
					// do nothing
					break;
				}
				// System.out.println("Received data (trimed): " + new String(data).trim());
			}
		});
		recvThread.start();

		connectedSignal.await();

		double totalDataSize = Double.valueOf(msgSize + 4) * total; // 4 for reversed int length value
		double dataMB = totalDataSize / (1024 * 1024);
		double dataGB = dataMB / 1024;

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < msgSize; i++) {
			sb.append("a");
		}

		String msgStr = sb.toString();

		System.out.println("Message size: " + df.format(msgSize) + " Bytes"
				+ (msgSize >= 1024
						? (" == " + (msgSize > 1024 * 1024 ? (df.format(Double.valueOf(msgSize) / 1024 / 1024) + "MB")
								: (df.format(Double.valueOf(msgSize) / 1024) + "KB")))
						: ""));
		System.out.println("Number of messages: " + df.format(total));
		System.out.println("Total size: " + df.format(dataMB) + "MB"
				+ (dataMB >= 1024 ? (" == " + df.format(dataGB) + "GB") : ""));

		connectedSignal.await();
		recvThread.interrupt();

		ByteBuffer length = ByteBuffer.allocate(4);
		// ByteBuffer body = ByteBuffer.allocateDirect(msgSize);
		String str = sb.toString();
		timeWatcher.reset();

		int count = total;
		while (count-- > 0) {
			length.clear();
			length.putInt(str.length());
			byte[] lengthBytes = length.array();
			socket.send(lengthBytes, ZMQ.NOBLOCK);
			// body.clear();
			// char[] chars = UnsafeUtils.getStringValue(msgStr);
			// CharBuffer cb = CharBuffer.wrap(chars);
			// encoder.encode(cb, body, true);
			// socket.sendZeroCopy(body, msgSize, ZMQ.NOBLOCK);

			socket.send(msgStr.getBytes(), ZMQ.NOBLOCK);
		}

		long timeNano = timeWatcher.getNano();
		double timeMillis = Double.valueOf(timeNano) / 1e6;

		timeNano = timeWatcher.endLapNano();
		timeMillis = Double.valueOf(timeNano) / 1e6;

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

		System.exit(0);
	}
}