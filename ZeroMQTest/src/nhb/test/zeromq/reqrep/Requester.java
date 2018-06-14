package nhb.test.zeromq.reqrep;

import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.ZeroMQTest;

public class Requester extends ZeroMQTest {

	public static void main(String[] args) {
		new Requester().runTest();
	}

	private void send() {
		ZMQSocket socket = this.openSocket("tcp://127.0.0.1:8787", ZMQSocketType.REQ_CONNECT);

		int numMessages = (int) 1e5;
		long startTime = System.nanoTime();
		int i = 0;
		for (; i < numMessages; i++) {
			if (socket.send("ping")) {
				socket.recv();
			} else {
				System.out.println("Cannot send message");
				break;
			}
		}
		Long totalTime = System.nanoTime() - startTime;
		System.out.println("message per sec: " + i / (totalTime.doubleValue() / 1e9));
	}

	@Override
	protected void test() throws Exception {
		int numThreads = 7;
		Thread[] threads = new Thread[numThreads];
		for (int i = 0; i < numThreads; i++) {
			threads[i] = new Thread(() -> {
				send();
			});
		}

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			for (Thread thread : threads) {
				if (thread.isAlive()) {
					thread.interrupt();
				}
			}
		}));

		for (Thread thread : threads) {
			thread.start();
		}
	}
}
