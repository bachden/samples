package nhb.test.zeromq.stream.server;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.dsl.ProducerType;
import com.nhb.common.data.PuElement;
import com.nhb.common.vo.ByteArrayWrapper;
import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

import nhb.test.zeromq.ZeroMQTest;

public class DisruptorStreamServer extends ZeroMQTest {

	public static void main(String[] args) {
		new DisruptorStreamServer().runTest();
	}

	private static final int BUFFER_SIZE = 4096;

	private EventFactory<MessagePieceEvent> eventFactory = MessagePieceEvent.newFactory();

	private ThreadFactory threadFactory = new ThreadFactory() {

		private final AtomicInteger idSeed = new AtomicInteger(0);

		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r, String.format("Thread #%d", this.idSeed.getAndIncrement()));
		}
	};
	private ExceptionHandler<MessagePieceEvent> exceptionHandler = new ExceptionHandler<MessagePieceEvent>() {

		@Override
		public void handleEventException(Throwable ex, long sequence, MessagePieceEvent event) {
			System.out.println("error: " + ex);
		}

		@Override
		public void handleOnStartException(Throwable ex) {
			ex.printStackTrace();
		}

		@Override
		public void handleOnShutdownException(Throwable ex) {
			ex.printStackTrace();
		}
	};

	@SuppressWarnings("unchecked")
	private ZMQSocketEventProducer initReceivingWorkers(int numWorkers, EventHandler<MessagePieceEvent> eventHandler) {

		RingBuffer<MessagePieceEvent>[] ringBuffers = new RingBuffer[numWorkers];
		SequenceBarrier[] barries = new SequenceBarrier[numWorkers];

		Thread[] threads = new Thread[numWorkers + 1];

		for (int i = 0; i < numWorkers; i++) {
			ZMQStreamSocketReceiver receiver = new ZMQStreamSocketReceiver();

			RingBuffer<MessagePieceEvent> ringBuffer = RingBuffer.createSingleProducer(eventFactory, BUFFER_SIZE);
			BatchEventProcessor<MessagePieceEvent> processor = new BatchEventProcessor<>(ringBuffer,
					ringBuffer.newBarrier(), receiver);

			processor.setExceptionHandler(exceptionHandler);

			ringBuffers[i] = ringBuffer;
			threads[i] = threadFactory.newThread(processor);
			barries[i] = ringBuffer.newBarrier(processor.getSequence());
		}

		MultiBufferBatchEventProcessor<MessagePieceEvent> processor = new MultiBufferBatchEventProcessor<MessagePieceEvent>(
				ringBuffers, barries, eventHandler);

		for (int i = 0; i < ringBuffers.length; i++) {
			ringBuffers[i].addGatingSequences(processor.getSequences()[i]);
		}

		threads[numWorkers] = threadFactory.newThread(processor);
		for (Thread thread : threads) {
			thread.start();
		}

		return new ZMQSocketEventProducer() {

			@Override
			public void shutdown() {
				for (Thread thread : threads) {
					thread.interrupt();
				}
			}

			private void publish(RingBuffer<MessagePieceEvent> ringBuffer, ByteArrayWrapper id, byte[] data) {
				long sequence = ringBuffer.next();
				try {
					MessagePieceEvent event = ringBuffer.get(sequence);
					event.setSequence(sequence);
					event.setId(id);
					event.setData(data);
					event.setExtractedMessages(null);
				} finally {
					ringBuffer.publish(sequence);
				}
			}

			@Override
			public void publish(ByteArrayWrapper id, byte[] data) {
				RingBuffer<MessagePieceEvent> ringBuffer = ringBuffers[id.hashCode() % numWorkers];
				this.publish(ringBuffer, id, data);
			}
		};
	}

	private ZMQSocketResponder initResponder(ZMQSocket socket) {
		return new ZMQSocketResponder(socket, 14, ProducerType.SINGLE);
	}

	@Override
	protected void test() throws Exception {

		Thread recvThread = new Thread(() -> {

			final ZMQSocket socket = this.openSocket("tcp://*:8787", ZMQSocketType.STREAM_BIND);

			final ZMQSocketResponder responder = initResponder(socket);

			final ZMQSocketEventProducer producer = this.initReceivingWorkers(4, new EventHandler<MessagePieceEvent>() {

				@Override
				public void onEvent(MessagePieceEvent event, long sequence, boolean endOfBatch) throws Exception {
					if (event.getExtractedMessages() != null) {
						for (PuElement message : event.getExtractedMessages()) {
							responder.send(event.getId().getSource(), message);
						}
					}
				}
			});

			responder.start();
			
			while (!Thread.currentThread().isInterrupted()) {
				byte[] id = new byte[5];

				int readBytes = socket.recv(id, 0, 5, 0);
				if (readBytes == 5) {
					ByteArrayWrapper wrappedId = ByteArrayWrapper.newInstance(id);
					byte[] data = socket.recv();
					if (data != null) {
						producer.publish(wrappedId, data);
					}
				}
			}

			socket.close();
			producer.shutdown();
			responder.shutdown();

		}, "Receiving Thread");

		recvThread.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			recvThread.interrupt();
		}));
	}
}
