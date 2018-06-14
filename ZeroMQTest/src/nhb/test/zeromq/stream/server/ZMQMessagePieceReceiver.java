package nhb.test.zeromq.stream.server;

public interface ZMQMessagePieceReceiver {

	void shutdown();

	void publish(int id, byte[] data);
}
