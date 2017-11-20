package nhb.test.zeromq;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.zeromq.ZMQ.Msg;

import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketRegistry;
import com.nhb.messaging.zmq.ZMQSocketType;

public class TestJNI {

	public static void main(String[] args) throws InterruptedException {
		ZMQSocketRegistry zmqSocketRegistry = new ZMQSocketRegistry();

		Thread serverThread = new Thread(() -> {
			final ZMQSocket server = zmqSocketRegistry.openSocket("tcp://*:8787", ZMQSocketType.SERVER);
			while (!Thread.currentThread().isInterrupted()) {
				Msg msg = (Msg) server.getSocket().recvMsg(0);
				ByteBuffer buffer = ByteBuffer.allocate(4);
				buffer.putInt(msg.getRoutingId());
				System.out.println("routing id as byte[]: " + Arrays.toString(buffer.array()));
				System.out.println(
						"Got msg with routing id: " + msg.getRoutingId() + ", data: " + Arrays.toString(msg.getData()));

				// msg.setRoutingId(0);
				msg.setData("Hello World!!!".getBytes());
				server.getSocket().sendMsg(msg, 0);
			}
			server.close();
		}, "Server");
		serverThread.start();

		Thread.sleep(500l);

		ZMQSocket socket = zmqSocketRegistry.openSocket("tcp://localhost:8787", ZMQSocketType.CLIENT);

		Msg msg = new Msg();
		// msg.setRoutingId(1);
		msg.setData(new byte[] { 0, 1, 2, 3 });
		socket.getSocket().sendMsg(msg, 0);

		String recvStr = socket.recvStr(Charset.defaultCharset());
		System.out.println("recv: " + recvStr);

		socket = zmqSocketRegistry.openSocket("tcp://localhost:8787", ZMQSocketType.CLIENT);

		msg = new Msg();
		// msg.setRoutingId(1);
		msg.setData(new byte[] { 0, 1, 2, 4 });
		socket.getSocket().sendMsg(msg, 0);

		Thread.sleep(100l);
		socket.close();
		serverThread.interrupt();

		Thread.sleep(100);
		System.exit(0);

	}
}
