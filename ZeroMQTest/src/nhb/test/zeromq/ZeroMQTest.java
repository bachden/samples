package nhb.test.zeromq;

import org.zeromq.ZMQ.Socket;

import com.nhb.common.Loggable;
import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketRegistry;
import com.nhb.messaging.zmq.ZMQSocketType;

import lombok.AccessLevel;
import lombok.Getter;

public abstract class ZeroMQTest implements Loggable {

	@Getter(AccessLevel.PROTECTED)
	private final ZMQSocketRegistry socketRegistry = new ZMQSocketRegistry(4, true);

	public ZMQSocket openSocket(String addr, ZMQSocketType type) {
		return socketRegistry.openSocket(addr, type);
	}

	public void closeSocket(Socket socket) {
		socketRegistry.closeSocket(socket);
	}

	public final void runTest() {
		try {
			this.test();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected abstract void test() throws Exception;
}
