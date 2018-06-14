package nhb.test.zeromq.stream.client;

import java.nio.ByteBuffer;
import java.util.List;

import org.zeromq.ZMQ.Msg;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventReleaseAware;
import com.lmax.disruptor.EventReleaser;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.nhb.common.Loggable;
import com.nhb.common.data.PuElement;
import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketFactory;

import lombok.Setter;
import nhb.test.zeromq.stream.MessagePieceCumulator;

public class ZMQStreamSocketSender implements WorkHandler<MessageBufferEvent>, EventReleaseAware, Loggable {

	private static final int INTEGER_TYPE_SIZE = Integer.BYTES;
	@Setter
	private EventReleaser eventReleaser;

	public static ZMQStreamSocketSender[] createHandlers(int sendingPoolSize, int sendingRingBufferSize,
			ZMQSocketFactory socketFactory, int handlingPoolSize, int handlingBufferSize,
			ZMQSocketMessageHandler handler) {

		// indicate bufferSize = 2^exponent
		final int exponent = 13;

		if (sendingPoolSize > 0) {
			ZMQStreamSocketSender[] results = new ZMQStreamSocketSender[sendingPoolSize];
			for (int i = 0; i < sendingPoolSize; i++) {
				try {
					final ZMQSocket socket = socketFactory.newSocket();
					results[i] = new ZMQStreamSocketSender(socket, sendingRingBufferSize, exponent, handlingPoolSize,
							handlingBufferSize, handler);
				} catch (Exception e) {
					throw new RuntimeException("Exception while borrow object from socket pool", e);
				}
			}
			return results;
		}
		throw new IllegalArgumentException("Worker pool size cannot be negative or zero");
	}

	private final ByteBuffer buffer;
	private final ZMQSocket socket;

	private final int entrySize;
	private Thread receiver;

	private ZMQStreamSocketSender(ZMQSocket socket, int ringBufferSize, int exponent, int handlingPoolSize,
			int handlingBufferSize, ZMQSocketMessageHandler handler) {
		this.socket = socket;

		this.entrySize = Double.valueOf(Math.pow(2, exponent)).intValue();
		this.buffer = ByteBuffer.allocate(entrySize);

		this.initReceiver(handlingPoolSize, handlingBufferSize, handler);
	}

	private void initReceiver(int handlingPoolSize, int handlingBufferSize, ZMQSocketMessageHandler handler) {
		final MessagePieceCumulator cumulator = new MessagePieceCumulator();
		Disruptor<PuElementWrapper> disruptor = new Disruptor<>(new EventFactory<PuElementWrapper>() {

			@Override
			public PuElementWrapper newInstance() {
				return new PuElementWrapper();
			}
		}, handlingBufferSize, new ThreadFactoryBuilder().setNameFormat("Socket Message Handler #%d").build(),
				handlingPoolSize == 1 ? ProducerType.SINGLE : ProducerType.MULTI, new YieldingWaitStrategy());

		@SuppressWarnings("unchecked")
		WorkHandler<PuElementWrapper>[] workers = new WorkHandler[handlingPoolSize];
		for (int i = 0; i < workers.length; i++) {
			workers[i] = new WorkHandler<PuElementWrapper>() {

				@Override
				public void onEvent(PuElementWrapper event) throws Exception {
					handler.onMessage(event.getData());
				}
			};
		}

		disruptor.handleEventsWithWorkerPool(workers);

		RingBuffer<PuElementWrapper> ringBuffer = disruptor.start();

		receiver = new Thread(() -> {
			while (!Thread.currentThread().isInterrupted()) {
				Msg msg = (Msg) socket.recvMsg(0);
				if (msg.getData() != null && msg.getData().length > 0) {
					List<PuElement> messages = cumulator.receive(msg.getData());
					if (messages != null && !messages.isEmpty()) {
						for (PuElement message : messages) {
							long sequence = ringBuffer.next();
							try {
								PuElementWrapper event = ringBuffer.get(sequence);
								event.setData(message);
							} finally {
								ringBuffer.publish(sequence);
							}
						}
					}
				}
				// Thread.yield();
			}
		});
		receiver.start();
	}

	public void shutdown() {
		this.receiver.interrupt();
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
		buffer.position(trunkSize);

		// this.socket.send(this.socketId, ZMQ.SNDMORE);
		if (!this.socket.send(buffer.array(), 0, trunkSize, 0)) {
			throw new RuntimeException("Message couldn't be sent");
		}

		// if (!this.socket.sendZeroCopy(buffer, trunkSize, 0)) {
		// throw new RuntimeException("Message couldn't be sent");
		// }

		this.eventReleaser.release();
	}
}