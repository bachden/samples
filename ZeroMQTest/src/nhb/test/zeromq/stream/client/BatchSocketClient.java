package nhb.test.zeromq.stream.client;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ThreadFactory;

import org.zeromq.ZMQ.Msg;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.nhb.common.Loggable;
import com.nhb.common.data.PuElement;
import com.nhb.messaging.zmq.ZMQSocket;

import lombok.Builder;
import lombok.Getter;
import nhb.test.zeromq.stream.MessagePieceCumulator;

public class BatchSocketClient implements Loggable {

	private static final int INTEGER_SIZE = Integer.BYTES;

	@Getter
	@Builder
	public static class ReceiverConfig {
		@Builder.Default
		private int bufferSize = 1024;
		@Builder.Default
		private int poolSize = 1;
		@Builder.Default
		private String threadNamePattern = "Socket client receiver #%d";
		private ZMQSocketMessageHandler handler;
	}

	@Getter
	@Builder
	public static class SenderConfig {
		@Builder.Default
		private int batchingSize = 1000;
		@Builder.Default
		private int bufferSize = 1024;
		@Builder.Default
		private WaitStrategy waitStrategy = new YieldingWaitStrategy();
		@Builder.Default
		private ProducerType producerType = ProducerType.SINGLE;
		@Builder.Default
		private String threadName = "Socket client sender";
	}

	private final ZMQSocket socket;
	private RingBuffer<MessageBufferEvent> sender;
	private Thread sendingThread;
	private Thread receiverThread;
	private int maxBatchSize = 1000;

	private EventHandler<MessageBufferEvent> sendingEventHandler = new EventHandler<MessageBufferEvent>() {

		private final ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
		private int currentBatchSize = 0;

		{
			this.reset();
		}

		private void reset() {
			buffer.clear();
			buffer.position(INTEGER_SIZE);
		}

		@Override
		public void onEvent(MessageBufferEvent event, long sequence, boolean endOfBatch) throws Exception {
			// if (endOfBatch) {
			// getLogger().debug("Got end of batch, current batch size={}",
			// currentBatchSize);
			// }
			boolean send = endOfBatch;
			boolean rewriteCurrentEvent = false;

			buffer.mark();
			try {
				event.getMessage().writeTo(new ByteBufferOutputStream(buffer));
				if (++currentBatchSize >= maxBatchSize) {
					send = true;
					getLogger().debug("Reach max batch size={}", maxBatchSize);
				}
			} catch (BufferOverflowException ex) {
				this.buffer.reset();
				if (buffer.position() == INTEGER_SIZE) {
					throw new RuntimeException("Message size too large");
				}
				// getLogger().debug("Buffer overflow, current batch size={}",
				// currentBatchSize);
				send = true;
				rewriteCurrentEvent = true;
			}

			if (send || !buffer.hasRemaining()) {
				int trunkSize = buffer.position();
				// getLogger().debug("Sending data, trunk size={}", trunkSize);
				this.buffer.rewind();
				this.buffer.putInt(trunkSize - INTEGER_SIZE);
				this.buffer.position(trunkSize);

				try {
					if (!socket.send(buffer.array(), 0, buffer.position(), 0)) {
						throw new RuntimeException("Cannot send messages");
					}
				} finally {
					this.reset();
					currentBatchSize = 0;
				}
			}

			if (rewriteCurrentEvent) {
				this.onEvent(event, sequence, endOfBatch);
			}
		}
	};

	public BatchSocketClient(ZMQSocket socket, SenderConfig senderConfig, ReceiverConfig receiverConfig) {
		this.socket = socket;
		this.initSender(senderConfig);
		this.initReceiver(receiverConfig);
		this.maxBatchSize = senderConfig.getBatchingSize();
	}

	public void start() {
		this.sendingThread.start();
		this.receiverThread.start();
	}

	public void stop() {
		this.sendingThread.interrupt();
		this.receiverThread.interrupt();
	}

	public void send(PuElement... messages) {
		long maxSequence = this.sender.next(messages.length);
		try {
			long sequence = maxSequence - messages.length + 1;
			for (int i = 0; i < messages.length; i++) {
				MessageBufferEvent event = this.sender.get(sequence++);
				event.setMessage(messages[i]);
			}
		} finally {
			this.sender.publish(maxSequence - messages.length + 1, maxSequence);
		}
	}

	private void initSender(SenderConfig senderConfig) {
		this.sender = RingBuffer.create(senderConfig.producerType, MessageBufferEvent.newFactory(),
				senderConfig.bufferSize, senderConfig.waitStrategy);

		BatchEventProcessor<MessageBufferEvent> processor = new BatchEventProcessor<>(this.sender,
				this.sender.newBarrier(), sendingEventHandler);

		this.sendingThread = new Thread(processor, senderConfig.threadName);
		this.sender.addGatingSequences(processor.getSequence());
	}

	private void initReceiver(final ReceiverConfig receiverConfig) {

		ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(receiverConfig.getThreadNamePattern())
				.build();

		final Disruptor<PuElementWrapper> receiver = new Disruptor<>(new EventFactory<PuElementWrapper>() {

			@Override
			public PuElementWrapper newInstance() {
				return new PuElementWrapper();
			}
		}, receiverConfig.bufferSize, threadFactory, ProducerType.SINGLE, new YieldingWaitStrategy());

		@SuppressWarnings("unchecked")
		WorkHandler<PuElementWrapper>[] workers = new WorkHandler[receiverConfig.poolSize];
		for (int i = 0; i < workers.length; i++) {
			workers[i] = new WorkHandler<PuElementWrapper>() {

				@Override
				public void onEvent(PuElementWrapper event) throws Exception {
					receiverConfig.handler.onMessage(event.getData());
				}
			};
		}

		receiver.handleEventsWithWorkerPool(workers);

		this.receiverThread = new Thread(() -> {
			final RingBuffer<PuElementWrapper> ringBuffer = receiver.start();
			final MessagePieceCumulator cumulator = new MessagePieceCumulator();
			while (!Thread.currentThread().isInterrupted()) {
				Msg msg = (Msg) socket.recvMsg(0);
				if (msg.getData() != null) {
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
			}
			receiver.shutdown();
		});
	}
}
