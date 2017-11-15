package nhb.test.zeromq.stream.client;

import java.nio.ByteBuffer;
import java.util.List;

import org.zeromq.ZMQ;

import com.lmax.disruptor.EventReleaseAware;
import com.lmax.disruptor.EventReleaser;
import com.lmax.disruptor.WorkHandler;
import com.nhb.common.Loggable;
import com.nhb.common.data.PuElement;
import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketFactory;

import lombok.Setter;
import nhb.test.zeromq.stream.server.MessagePieceCumulator;

public class ZMQStreamSocketSender implements WorkHandler<MessageBufferEvent>, EventReleaseAware, Loggable {

	private static final int INTEGER_TYPE_SIZE = Integer.BYTES;
	@Setter
	private EventReleaser eventReleaser;

	public static ZMQStreamSocketSender[] createHandlers(int poolSize, int ringBufferSize,
			ZMQSocketFactory socketFactory) {

		final int exponent = 13;
		final int mask = ringBufferSize * 2 - 1;

		if (poolSize > 0) {
			ZMQStreamSocketSender[] results = new ZMQStreamSocketSender[poolSize];
			for (int i = 0; i < poolSize; i++) {
				try {
					final ZMQSocket socket = socketFactory.newSocket();
					final byte[] id = new byte[5];
					socket.recv(id, 0, 5, 0);
					results[i] = new ZMQStreamSocketSender(id, socket, ringBufferSize, mask, exponent);
				} catch (Exception e) {
					throw new RuntimeException("Exception while borrow object from socket pool", e);
				}
			}
			return results;
		}
		throw new IllegalArgumentException("Worker pool size cannot be negative or zero");
	}

	private final ByteBuffer buffer;
	private final byte[] socketId;
	private final ZMQSocket socket;

	private final int entrySize;
	// private final int mask;
	// private final int exponent;

	private ZMQStreamSocketSender(byte[] id, ZMQSocket socket, int ringBufferSize, int mask, int exponent) {
		this.socketId = id;
		this.socket = socket;

		// this.mask = mask;
		// this.exponent = exponent;
		this.entrySize = Double.valueOf(Math.pow(2, exponent)).intValue();
		this.buffer = ByteBuffer.allocate(entrySize);

		this.initReceiver();
	}

	private void initReceiver() {
		MessagePieceCumulator cumulator = new MessagePieceCumulator();
		Thread receiver = new Thread(() -> {
			byte[] id = new byte[5];
			while (!Thread.currentThread().isInterrupted()) {
				socket.recv(id, 0, 5, 0);
				List<PuElement> messages = cumulator.receive(socket.recv());
				getLogger().debug("Got messages: {}", messages);
			}
		});
		receiver.start();
	}

	@Override
	public void onEvent(MessageBufferEvent event) throws Exception {

		buffer.clear();
		buffer.mark();

		// reserved 4 bytes for length prepend
		buffer.position(buffer.position() + INTEGER_TYPE_SIZE);
		event.getMessage().writeTo(new ByteBufferOutputStream(buffer));

		final int trunkSize = buffer.position();
		buffer.reset();
		buffer.putInt(trunkSize - INTEGER_TYPE_SIZE);

		this.socket.send(this.socketId, ZMQ.SNDMORE);
		if (!this.socket.send(buffer.array(), 0, trunkSize, ZMQ.NOBLOCK)) {
			throw new RuntimeException("Message couldn't be sent");
		}

		this.eventReleaser.release();
	}
}