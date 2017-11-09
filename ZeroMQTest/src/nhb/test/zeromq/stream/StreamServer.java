package nhb.test.zeromq.stream;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;

import com.nhb.common.utils.TimeWatcher;
import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.ZeroMQTest;

public class StreamServer extends ZeroMQTest {

	public static void main(String[] args) {
		new StreamServer().runTest();
	}

	@Override
	protected void test() throws Exception {
		final ZMQSocket socket = this.openSocket("tcp://*:8787", ZMQSocketType.STREAM_BIND);

		// final Set<ByteArrayWrapper> readyToSends = new HashSet<>();
		// Thread sender = new Thread(() -> {
		// String msg = "pong";
		// while (!Thread.currentThread().isInterrupted()) {
		// try {
		// Iterator<ByteArrayWrapper> it = readyToSends.iterator();
		// while (it.hasNext()) {
		// ByteArrayWrapper entry = it.next();
		// byte[] clientId = entry.getSource();
		// try {
		// if (socket.send(clientId, ZMQ.SNDMORE)) {
		// socket.send(msg + Arrays.toString(clientId) + "\n", 0);
		// }
		// } catch (ZMQException ex) {
		// it.remove();
		// }
		// }
		// Thread.sleep(500l);
		// } catch (Exception e) {
		// e.printStackTrace();
		// break;
		// }
		// }
		// }, "Sender");
		// sender.start();

		byte[] clientId = new byte[5];
		int total = (int) 1e6;
		AtomicInteger count = new AtomicInteger(total);
		new Thread(() -> {
			while (!Thread.currentThread().isInterrupted()) {
				try {
					Thread.sleep(1000l);
					// System.out.println("count: " + count.get());
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
			}
		}).start();
		TimeWatcher timeWatcher = new TimeWatcher();
		long totalTime = -1;
		// byte[] size = new byte[4];
		while (true) {
			socket.recv(clientId, 0, 5, 0);
			// ByteArrayWrapper wrappedClientId =
			// ByteArrayWrapper.newInstanceWithJNIHashCodeCalculator(clientId);
			// if (!readyToSends.contains(wrappedClientId)) {
			// readyToSends.add(wrappedClientId);
			// }
			// socket.recv(size, 0, 4, 0);
			// int length = ByteBuffer.wrap(size).getInt();
			// System.out.println("Length: " + length);
			byte[] data = socket.recv();
			System.out.println("got data: " + new String(data));
			// System.out.println("Received from id: " + Arrays.toString(clientId) + " ->
			// body: " + new String(body));
			final int currCount = count.getAndDecrement();
			if (currCount == total) {
				timeWatcher.reset();
			} else if (currCount == 1) {
				totalTime = timeWatcher.endLapNano();
				break;
			}
		}
		System.out.println("TPS: " + new DecimalFormat("###,###.##").format(Double.valueOf(total) * 1e9 / totalTime));
	}

}
