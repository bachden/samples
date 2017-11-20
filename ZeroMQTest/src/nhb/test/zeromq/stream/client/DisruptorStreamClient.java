package nhb.test.zeromq.stream.client;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.nhb.common.Loggable;
import com.nhb.common.data.PuElement;
import com.nhb.common.data.PuObject;
import com.nhb.common.utils.TimeWatcher;
import com.nhb.messaging.zmq.ZMQSocketFactory;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.ZeroMQTest;

public class DisruptorStreamClient extends ZeroMQTest implements Loggable {
	private static final int BUFFER_SIZE = 4096;

	private EventFactory<MessageBufferEvent> eventFactory = MessageBufferEvent.newFactory();

	private final ThreadFactory threadFactory = new ThreadFactory() {

		private final AtomicInteger idSeed = new AtomicInteger(0);

		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r, String.format("Thread #%d", idSeed.getAndIncrement()));
		}
	};

	public static void main(String[] args) {
		new DisruptorStreamClient().runTest();
	}

	@Override
	protected void test() throws Exception {
		DecimalFormat df = new DecimalFormat("###,###.##");
		final TimeWatcher timeWatcher = new TimeWatcher();

		final int total = (int) 1e6;
		int numWorker = 3;

		PuObject msg = new PuObject();
		msg.setBoolean("boolVal", true);
		msg.setByte("byteVal", Byte.MAX_VALUE);
		msg.setShort("shortVal", Short.MAX_VALUE);
		msg.setInteger("intVal", Integer.MAX_VALUE);
		msg.setLong("longVal", Long.MAX_VALUE);
		msg.setFloat("floatVal", Float.MAX_VALUE);
		msg.setDouble("doubleVal", Double.MAX_VALUE);
		msg.setString("stringVal", "Nguyễn Hoàng Bách1");

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

		Disruptor<MessageBufferEvent> disruptor = new Disruptor<MessageBufferEvent>(eventFactory //
				, BUFFER_SIZE //
				, threadFactory //
				, ProducerType.SINGLE //
				, new YieldingWaitStrategy());

		final CountDownLatch doneSignal = new CountDownLatch(1);
		ZMQStreamSocketSender[] handlers = ZMQStreamSocketSender.createHandlers(numWorker, BUFFER_SIZE,
				new ZMQSocketFactory(this.getSocketRegistry(), "tcp://localhost:8787", ZMQSocketType.CLIENT), 1,
				BUFFER_SIZE, new ZMQSocketMessageHandler() {

					private AtomicInteger countDown = new AtomicInteger(total);

					@Override
					public void onMessage(PuElement message) {
						if (this.countDown.decrementAndGet() == 0) {
							doneSignal.countDown();
						} else {
							// if (countDown % 100 == 0) {
							// getLogger().debug("Remaining: " + countDown);
							// }
						}
					}
				});

		disruptor.handleEventsWithWorkerPool(handlers);

		disruptor.setDefaultExceptionHandler(new ExceptionHandler<MessageBufferEvent>() {

			@Override
			public void handleEventException(Throwable ex, long sequence, MessageBufferEvent event) {
				ex.printStackTrace();
			}

			@Override
			public void handleOnStartException(Throwable ex) {
				ex.printStackTrace();
			}

			@Override
			public void handleOnShutdownException(Throwable ex) {
				ex.printStackTrace();
			}
		});

		disruptor.start();
		System.out.println("******** Disruptor started **********");

		timeWatcher.reset();

		int count = total;
		while (count-- > 0) {
			disruptor.publishEvent(MessageBufferEvent.TRANSLATOR, msg);
		}

		long timeNano = timeWatcher.getNano();
		double timeMillis = Double.valueOf(timeNano) / 1e6;

		System.out.println("Publishing elapsed time: " + df.format(timeMillis) + " milliseconds"
				+ (timeMillis > 1000 ? (" == " + df.format(timeMillis / 1000) + " seconds") : ""));

		doneSignal.await();

		timeNano = timeWatcher.endLapNano();
		timeMillis = Double.valueOf(timeNano) / 1e6;

		System.out.println("*********** DONE ***********");
		System.out.println("Num workers: " + numWorker);
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

		// Thread.sleep(1000l);
		// disruptor.shutdown();
		// System.exit(0);
	}
}