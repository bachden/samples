package nhb.test.zeromq.pushpull;

import java.nio.ByteBuffer;

import com.nhb.common.data.msgpkg.PuElementTemplate;
import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.stream.server.ByteBufferInputStream;

public class Pull extends PushPullBaseTest {

	public static void main(String[] args) {
		new Pull().runTest();
	}

	@Override
	protected void test() throws Exception {
		ZMQSocket socket = this.openSocket(ENDPOINT, ZMQSocketType.PULL_BIND);
		socket.setHWM((long) 1e6);
		socket.setRcvHWM((long) 1e6);

		ByteBuffer bb = ByteBuffer.allocateDirect(1024);

		while (Thread.currentThread().isAlive()) {
			bb.clear();
			if (socket.recvZeroCopy(bb, bb.capacity(), 0) == -1) {
				throw new Exception("[JNI] Cannot get byte buffer direct address");
			} else {
				bb.flip();
				try {
					getLogger().debug("got: {}", PuElementTemplate.getInstance().read(new ByteBufferInputStream(bb)));
				} catch (Exception ex) {
					getLogger().error("Exception: ", ex);
				}
			}
		}
	}
}
