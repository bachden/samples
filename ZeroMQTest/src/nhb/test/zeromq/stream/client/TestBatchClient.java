package nhb.test.zeromq.stream.client;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.nhb.common.Loggable;
import com.nhb.common.data.PuElement;
import com.nhb.common.data.PuObject;
import com.nhb.common.utils.TimeWatcher;
import com.nhb.messaging.zmq.ZMQSocketFactory;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.ZeroMQTest;
import nhb.test.zeromq.stream.client.BatchSocketClient.ReceiverConfig;
import nhb.test.zeromq.stream.client.BatchSocketClient.SenderConfig;

public class TestBatchClient extends ZeroMQTest implements Loggable {
	private static final int BUFFER_SIZE = 4096;

	public static void main(String[] args) {
		new TestBatchClient().runTest();
	}

	@Override
	protected void test() throws Exception {
		final ZMQSocketFactory socketFactory = new ZMQSocketFactory(this.getSocketRegistry(), "tcp://localhost:8787",
				ZMQSocketType.CLIENT);
		DecimalFormat df = new DecimalFormat("###,###.##");
		final TimeWatcher timeWatcher = new TimeWatcher();

		final int total = (int) 1e6;
		int numWorkers = 1;

		PuObject msg = new PuObject();
		msg.setBoolean("boolVal", true);
		msg.setByte("byteVal", Byte.MAX_VALUE);
		msg.setShort("shortVal", Short.MAX_VALUE);
		msg.setInteger("intVal", Integer.MAX_VALUE);
		msg.setLong("longVal", Long.MAX_VALUE);
		msg.setFloat("floatVal", Float.MAX_VALUE);
		msg.setDouble("doubleVal", Double.MAX_VALUE);
		msg.setString("stringVal", "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách" //
				+ "Nguyễn Hoàng Bách");

		// just calculate length of data
		byte[] msgBytes = msg.toBytes();
		getLogger().debug("Message to be sent: {}", Arrays.toString(msgBytes));
		int msgSize = msgBytes.length;
		msgBytes = null;
		// done calculate

		double totalDataSize = Double.valueOf(msgSize + 4) * total; // 4 for reversed int length value
		double dataMB = totalDataSize / (1024 * 1024);
		double dataGB = dataMB / 1024;

		System.out.println("Message size: " + df.format(msgSize) + " Bytes"
				+ (msgSize >= 1024
						? (" == " + (msgSize > 1024 * 1024 ? (df.format(Double.valueOf(msgSize) / 1024 / 1024) + "MB")
								: (df.format(Double.valueOf(msgSize) / 1024) + "KB")))
						: ""));
		System.out.println("Number of messages: " + df.format(total));
		System.out.println("Total size: " + df.format(dataMB) + "MB"
				+ (dataMB >= 1024 ? (" == " + df.format(dataGB) + "GB") : ""));

		final CountDownLatch doneSignal = new CountDownLatch(1);
		BatchSocketClient client = new BatchSocketClient( //
				socketFactory.newSocket(), //
				SenderConfig.builder().bufferSize(BUFFER_SIZE).build(), //
				ReceiverConfig.builder().bufferSize(BUFFER_SIZE).handler(new ZMQSocketMessageHandler() {
					private final AtomicInteger count = new AtomicInteger(total);

					@Override
					public void onMessage(PuElement message) {
						// getLogger().debug("Received: {}", message);
						if (count.decrementAndGet() == 0) {
							doneSignal.countDown();
						}
					}
				}).build());

		client.start();

		getLogger().debug("*********** Socket client started ***********");

		timeWatcher.reset();

		int count = total;
		while (count-- > 0) {
			client.send(msg);
		}

		long timeNano = timeWatcher.getElapsedNano();
		double timeMillis = Double.valueOf(timeNano) / 1e6;

		System.out.println("Publishing elapsed time: " + df.format(timeMillis) + " milliseconds"
				+ (timeMillis > 1000 ? (" == " + df.format(timeMillis / 1000) + " seconds") : ""));

		doneSignal.await();

		timeNano = timeWatcher.endLapNano();
		timeMillis = Double.valueOf(timeNano) / 1e6;

		System.out.println("*********** DONE ***********");
		System.out.println("Num workers: " + numWorkers);
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

		client.stop();
	}
}