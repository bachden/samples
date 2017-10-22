package nhb.test.zeromq.pushpull;

import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.ZeroMQTest;

public class Pull extends ZeroMQTest {

	public static void main(String[] args) {
		new Pull().runTest();
	}

	@Override
	protected void test() throws Exception {
		ZMQSocket socket = this.openSocket("tcp://127.0.0.1:4567", ZMQSocketType.PULL_CONSUMER);
		while (true) {
			System.out.println(new String(socket.recv()));
		}
	}
}
