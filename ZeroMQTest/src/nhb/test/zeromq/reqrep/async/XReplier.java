package nhb.test.zeromq.reqrep.async;

import java.util.ArrayList;
import java.util.Collection;

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
			final ZMQSocket socket = this.openSocket(INPROC_WORKERS, ZMQSocketType.REP_CONNECT);
			threads[i] = new Thread(() -> {
				while (true) {
					socket.recv();
					// System.out.println(req + " to " + Thread.currentThread().getName());
					socket.send("pong");
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
		ZMQSocket router = this.openSocket(TCP_8787, ZMQSocketType.ROUTER_BIND);
		ZMQSocket dealer = this.openSocket(INPROC_WORKERS, ZMQSocketType.DEALER_BIND);

		Collection<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 1; i++) {
			threads.add(router.asyncForwardTo(dealer));
		}

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			for (Thread forwardingThread : threads) {
				forwardingThread.interrupt();
			}
		}));

		System.out.println("Started");
	}
}
