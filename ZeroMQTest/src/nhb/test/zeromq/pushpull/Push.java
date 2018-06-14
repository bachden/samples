package nhb.test.zeromq.pushpull;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.concurrent.CountDownLatch;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.nhb.common.data.PuObject;
import com.nhb.common.utils.TimeWatcher;
import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketFactory;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.stream.client.ByteBufferOutputStream;

public class Push extends PushPullBaseTest {

	public static void main(String[] args) {
		new Push().runTest();
	}

	@Override
	protected void test() throws Exception {

		final ZMQSocketFactory socketFactory = new ZMQSocketFactory(this.getSocketRegistry(), ENDPOINT,
				ZMQSocketType.PUSH_CONNECT);

		int numThreads = 1;
		int numMessagePerThread = (int) 5e6;

		Thread[] threads = new Thread[numThreads];
		int[] counters = new int[numThreads];
		int[] dataSizes = new int[numThreads];

		final CountDownLatch startSignal = new CountDownLatch(1);
		for (int i = 0; i < threads.length; i++) {
			final int threadId = i;
			counters[i] = 0;
			dataSizes[i] = 0;
			threads[i] = new Thread(() -> {
				getLogger().debug("Prepare data to send");

				ZMQSocket socket = socketFactory.newSocket();
				socket.setHWM(numMessagePerThread);
				socket.setSndHWM(numMessagePerThread);

				try {
					// wait for socket to connect
					Thread.sleep(10);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				final ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

				try {
					startSignal.await();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				getLogger().debug("Start sending");
				int msgSize;
				for (int j = 0; j < numMessagePerThread; j++) {

					PuObject message = new PuObject();
					message.set("name", "Nguyễn Hoàng Bách");
					message.set("age", 29);
					message.set("id", j);
					message.set("threadId", threadId);

					buffer.clear();
					message.writeTo(new ByteBufferOutputStream(buffer));
					buffer.flip();

					try {
						if (socket.sendZeroCopy(buffer, msgSize = buffer.remaining(), ZMQ.NOBLOCK)) {
							counters[threadId] += 1;
							dataSizes[threadId] += msgSize;
						} else {
							getLogger().debug("Cannot send message: {}", message);
						}
					} catch (ZMQException ex) {
						getLogger().error("Error while sending message", ex);
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							break;
						}
					}
				}
				getLogger().debug("Sending done on thread " + Thread.currentThread().getName());
				// socket.close(10);
			}, "Sender #" + threadId);
			threads[i].start();
		}

		Thread monitorThread = new Thread(() -> {
			long totalMessages = numThreads * numMessagePerThread;
			DecimalFormat df = new DecimalFormat("0.##%");
			while (!Thread.currentThread().isInterrupted()) {
				int currCount = 0;
				for (int count : counters) {
					currCount += count;
				}
				getLogger("pureLogger").debug("Complete: " + currCount + " / " + totalMessages + " ==> "
						+ df.format(Double.valueOf(currCount) / totalMessages));

				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					break;
				}
			}
		}, "Monitor thread");
		monitorThread.setPriority(Thread.MAX_PRIORITY);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			for (Thread thread : threads) {
				thread.interrupt();
			}
			monitorThread.interrupt();
		}));

		monitorThread.start();

		Thread.sleep(100);

		TimeWatcher timeWatcher = new TimeWatcher();
		timeWatcher.reset();
		startSignal.countDown();

		for (Thread thread : threads) {
			thread.join();
		}

		long totalTimeNano = timeWatcher.endLapNano();
		monitorThread.interrupt();

		int sentMessagesCount = 0;
		for (int count : counters) {
			sentMessagesCount += count;
		}

		long sentDataSize = 0;
		for (int size : dataSizes) {
			sentDataSize += size;
		}

		DecimalFormat df = new DecimalFormat("###,###.##");
		System.out.println("******** STATISTIC ********");
		System.out.println("Sent " + sentDataSize + " bytes == " + df.format(Double.valueOf(sentDataSize) / 1024)
				+ "KB == " + df.format(Double.valueOf(sentDataSize) / 1024 / 1024) + "MB");

		System.out.println(
				"Num messages: " + sentMessagesCount + " success on total " + (numThreads * numMessagePerThread));
		System.out.println("Num threads: " + numThreads);
		System.out
				.println("Message avg size: " + df.format(Double.valueOf(sentDataSize) / sentMessagesCount) + " bytes");
		System.out
				.println("Rate: " + df.format(Double.valueOf(sentMessagesCount) / (Double.valueOf(totalTimeNano) / 1e9))
						+ " messages/sec");
		System.out.println("Throughput: "
				+ df.format((Double.valueOf(sentDataSize) / 1024 / 1024) / (totalTimeNano / 1e9)) + "MB/s");

		System.out.println("******* DONE *******");

		Thread.sleep(100);
		// System.exit(0);
	}

}
