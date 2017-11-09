package nhb.test.zeromq.stream;

import java.text.DecimalFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.zeromq.ZMQ;

import com.nhb.common.utils.TimeWatcher;
import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.ZeroMQTest;

public class StreamClient extends ZeroMQTest {

	public static void main(String[] args) {
		new StreamClient().runTest();
	}

	@Override
	protected void test() throws Exception {
		final ZMQSocket socket = this.openSocket("tcp://127.0.0.1:8787", ZMQSocketType.STREAM_CONNECT);

		CountDownLatch connectedSignal = new CountDownLatch(1);
		AtomicBoolean connected = new AtomicBoolean(false);
		byte[] myId = new byte[5];

		Thread recvThread = new Thread(() -> {
			while (!Thread.currentThread().isInterrupted()) {
				socket.recv(myId, 0, 5, 0);
				if (connected.compareAndSet(false, true)) {
					connectedSignal.countDown();
				}
				byte[] data = socket.recv();
				System.out.println("Received data (trimed): " + new String(data).trim());
			}
		});
		recvThread.start();

		connectedSignal.await();

		int total = (int) 1e6;
		int count = total;
		String msg = "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping"
				+ "pingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingpingping";

		msg += msg;
		msg += msg;
		msg += msg;
		msg += msg;
		msg += msg;
		msg += msg;
		msg += msg;

		Double totalDataSize = Double.valueOf(msg.length()) * total;
		System.out.println("Message size: " + msg.length() + " bytes --> total size (MB): "
				+ new DecimalFormat("###,###.##").format(totalDataSize / (1024 * 1024)));

		byte[] msgBytes = msg.getBytes();
		TimeWatcher timeWatcher = new TimeWatcher();
		timeWatcher.reset();
		while (count-- > 0) {
			// socket.send(new Big, ZMQ.SNDMORE);
			socket.send(msgBytes, ZMQ.NOBLOCK);
		}
		long timeNano = timeWatcher.endLapNano();
		DecimalFormat df = new DecimalFormat("###,###.##");
		double dataGB = totalDataSize / (1024 * 1024 * 1024);
		System.out.println("TPS: " + df.format(Double.valueOf(total) * 1e9 / timeNano) + ", time: "
				+ df.format(Double.valueOf(timeNano) / 1e6) + " milliseconds, throughput: "
				+ df.format(dataGB * 1e9 / timeNano) + " GB/s");

		recvThread.interrupt();
	}
}
