package nhb.test.zeromq.stream.server;

import com.nhb.common.vo.ByteArrayWrapper;

public interface ZMQSocketEventProducer {

	void shutdown();

	void publish(ByteArrayWrapper id, byte[] data);
}
