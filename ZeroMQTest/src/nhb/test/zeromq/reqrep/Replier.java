package nhb.test.zeromq.reqrep;

import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.ZeroMQTest;

public class Replier extends ZeroMQTest {

	public static void main(String[] args) {
		new Replier().runTest();
	}

	@Override
	protected void test() throws Exception {
		ZMQSocket socket = this.openSocket("tcp://127.0.0.1:8787", ZMQSocketType.REP);

		while (true) {
			socket.recv();
			socket.send("pong");
		}
	}
}
