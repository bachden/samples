package nhb.test.zeromq.reqrep.async;

import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.ZeroMQTest;

public class XReplier extends ZeroMQTest {

	private static final String TCP_8787 = "tcp://*:8787";
	private static final String INPROC_WORKERS = "inproc://workers";

	public static void main(String[] args) {
		new XReplier().runTest();
	}

	private void initWorkers() {
		int numWorkers = 2;
		final Thread[] threads = new Thread[numWorkers];
		for (int i = 0; i < threads.length; i++) {
			threads[i] = new Thread(() -> {
				ZMQSocket worker = openSocket(INPROC_WORKERS, ZMQSocketType.XREP);
				while (true) {
					worker.recv();
					// System.out.println(req + " to " + Thread.currentThread().getName());
					worker.send("pong");
				}
			}, "Worker #" + (i + 1));
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

	@Override
	protected void test() throws Exception {
		this.initWorkers();
		ZMQSocket router = this.openSocket(TCP_8787, ZMQSocketType.ROUTER);
		ZMQSocket dealer = this.openSocket(INPROC_WORKERS, ZMQSocketType.DEALER);

		router.proxy(dealer);
	}
}
