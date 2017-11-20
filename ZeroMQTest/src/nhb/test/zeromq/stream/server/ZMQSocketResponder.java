package nhb.test.zeromq.stream.server;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.zeromq.ZMQ.Msg;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventReleaser;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.nhb.common.Loggable;
import com.nhb.common.data.PuElement;
import com.nhb.messaging.zmq.ZMQSocket;

import lombok.Data;
import lombok.Setter;
import nhb.test.zeromq.stream.client.ByteBufferOutputStream;

public class ZMQSocketResponder implements Loggable {

	private static final int INTEGER_TYPE_SIZE = Integer.BYTES;
	@Setter
	private EventReleaser eventReleaser;

	private final ByteBuffer buffer;
	private final ZMQSocket socket;

	private final int entrySize;
	// private final int mask;
	// private final int exponent;

	@Data
	public static final class ResponseEvent {
		public static final EventFactory<ResponseEvent> FACTORY = new EventFactory<ZMQSocketResponder.ResponseEvent>() {

			@Override
			public ResponseEvent newInstance() {
				return new ResponseEvent();
			}
		};

		private int socketId;
		private PuElement message;
	}

	private EventHandler<ResponseEvent> responder = new EventHandler<ResponseEvent>() {

		@Override
		public void onEvent(ResponseEvent event, long sequence, boolean endOfBatch) throws Exception {
			buffer.clear();
			// ByteBuffer buffer = ByteBuffer.allocate(entrySize);
			buffer.mark();
			int offset = buffer.position();

			// reserved 4 bytes for length prepend
			buffer.position(buffer.position() + INTEGER_TYPE_SIZE);
			event.getMessage().writeTo(new ByteBufferOutputStream(buffer));

			final int trunkSize = buffer.position();
			buffer.reset();
			buffer.putInt(trunkSize - INTEGER_TYPE_SIZE);

			Msg msg = new Msg();
			msg.setData(buffer.array());
			msg.setRoutingId(event.getSocketId());
			msg.setLength(trunkSize);
			msg.setOffset(offset);

			if (!socket.sendMsg(msg, 0)) {
				getLogger().error("Send response message error", new RuntimeException("Message couldn't be sent"));
			} else {
				// getLogger().debug("Sent {} bytes data to {}", trunkSize,
				// Arrays.toString(event.getSocketId()));
			}
		}
	};

	private RingBuffer<ResponseEvent> ringBuffer;
	private Disruptor<ResponseEvent> disruptor;

	public ZMQSocketResponder(ZMQSocket socket, int exponent, ProducerType producerType) {
		this.socket = socket;

		// this.mask = mask;
		// this.exponent = exponent;
		this.entrySize = Double.valueOf(Math.pow(2, exponent)).intValue();
		this.buffer = ByteBuffer.allocate(entrySize);

		this.initDisruptor(producerType);
	}

	public void send(int socketId, PuElement message) {
		long sequence = ringBuffer.next();
		try {
			ResponseEvent event = this.ringBuffer.get(sequence);
			event.setSocketId(socketId);
			event.setMessage(message);
		} finally {
			this.ringBuffer.publish(sequence);
		}
	}

	@SuppressWarnings("unchecked")
	private void initDisruptor(ProducerType producerType) {
		Disruptor<ResponseEvent> disruptor = new Disruptor<>(ResponseEvent.FACTORY, 4096, new ThreadFactory() {

			private final AtomicInteger idSeed = new AtomicInteger(0);

			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, String.format("Responding thread #%d", idSeed.incrementAndGet()));
			}
		}, producerType, new YieldingWaitStrategy());

		disruptor.handleEventsWith(this.responder);
		this.disruptor = disruptor;
	}

	public void start() {
		this.ringBuffer = disruptor.start();
	}

	public void shutdown() {
		this.disruptor.shutdown();
	}

}