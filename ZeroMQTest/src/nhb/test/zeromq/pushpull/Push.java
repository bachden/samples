package nhb.test.zeromq.pushpull;

import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.ZeroMQTest;

public class Push extends ZeroMQTest {

	public static void main(String[] args) {
		new Push().runTest();
	}

	@Override
	protected void test() throws Exception {
		ZMQSocket socket = this.openSocket("tcp://127.0.0.1:4567", ZMQSocketType.PUSH_BIND);
		Thread.sleep(1000);
		System.out.println("Start sending...");
		for (int i = 1; i <= 10; i++) {
			socket.send("ping" + i);
		}
	}

}
