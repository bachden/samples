package nhb.test.zeromq.stream;

import java.text.DecimalFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.nhb.common.utils.TimeWatcher;
import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.ZeroMQTest;

public class StreamClient extends ZeroMQTest {

	private static final int ENTRY_SIZE = 4096;
	private static final int BUFFER_SIZE = 4096 * 16;

	private final ThreadFactory threadFactory = new ThreadFactory() {

		private final AtomicInteger idSeed = new AtomicInteger(0);

		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r, String.format("Thread #%d", idSeed.getAndIncrement()));
		}
	};;

	public static void main(String[] args) {
		new StreamClient().runTest();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void test() throws Exception {
		final ZMQSocket socket = this.openSocket("tcp://10.0.1.42:8787", ZMQSocketType.STREAM_CONNECT);

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

		final int total = (int) 1e6;
		int msgSize = 1024;

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < msgSize; i++) {
			sb.append("a");
		}

		DecimalFormat df = new DecimalFormat("###,###.##");

		double totalDataSize = Double.valueOf(msgSize + 4) * total; // 4 for reversed int length value
		double dataMB = totalDataSize / (1024 * 1024);
		double dataGB = dataMB / 1024;

		String msg = sb.toString();
		System.out.println("Message size: " + df.format(msgSize) + " Bytes"
				+ (msgSize >= 1024
						? (" == " + (msgSize > 1024 * 1024 ? (df.format(Double.valueOf(msgSize) / 1024 / 1024) + "MB")
								: (df.format(Double.valueOf(msgSize) / 1024) + "KB")))
						: ""));
		System.out.println("Number of messages: " + df.format(total));
		System.out.println("Total size: " + df.format(dataMB) + "MB"
				+ (dataMB >= 1024 ? (" == " + df.format(dataGB) + "GB") : ""));

		Disruptor<StringAsByteBufferEvent> disruptor = new Disruptor<StringAsByteBufferEvent>( //
				StringAsByteBufferEvent.newFactory(BUFFER_SIZE, ENTRY_SIZE)//
				, BUFFER_SIZE //
				, threadFactory//
				, ProducerType.SINGLE//
				, new BusySpinWaitStrategy());

		final CountDownLatch doneSignal = new CountDownLatch(1);

		disruptor.handleEventsWithWorkerPool(StringAsByteBufferEventPreparingWorker.createHandlers(3))
				.then(new EventHandler<StringAsByteBufferEvent>() {

					@Override
					public void onEvent(StringAsByteBufferEvent event, long sequence, boolean endOfBatch)
							throws Exception {
						socket.sendZeroCopy(event.getBuffer(), event.size(), ZMQ.NOBLOCK);
					}
				}).then(new EventHandler<StringAsByteBufferEvent>() {

					private int countDown = total;

					@Override
					public void onEvent(StringAsByteBufferEvent event, long sequence, boolean endOfBatch)
							throws Exception {
						if (--this.countDown == 0) {
							doneSignal.countDown();
						}
					}
				});

		disruptor.setDefaultExceptionHandler(new ExceptionHandler<StringAsByteBufferEvent>() {

			@Override
			public void handleEventException(Throwable ex, long sequence, StringAsByteBufferEvent event) {
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

		connectedSignal.await();
		recvThread.interrupt();

		disruptor.start();
		System.out.println("******** Disruptor started **********");

		TimeWatcher timeWatcher = new TimeWatcher();
		timeWatcher.reset();

		int count = total;
		while (count-- > 0) {
			disruptor.publishEvent(StringAsByteBufferEventTranslator.DEFAULT, msg);
		}

		long timeNano = timeWatcher.getNano();
		double timeMillis = Double.valueOf(timeNano) / 1e6;

		System.out.println("Publishing elasped time: " + df.format(timeMillis) + " milliseconds"
				+ (timeMillis > 1000 ? (" == " + df.format(timeMillis / 1000) + " seconds") : ""));

		doneSignal.await();

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

		disruptor.shutdown();
		System.exit(0);
	}
}