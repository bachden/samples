package nhb.test.zeromq.stream.server;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.zeromq.ZMQ.Msg;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.dsl.ProducerType;
import com.nhb.common.data.PuElement;
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
	private ZMQMessagePieceReceiver initReceiver(int numWorkers, EventHandlerInitializer handlerInitializer) {

		RingBuffer<MessagePieceEvent>[] ringBuffers = new RingBuffer[numWorkers];
		SequenceBarrier[] barriers = new SequenceBarrier[numWorkers];

		List<Thread> threads = new LinkedList<>();

		for (int i = 0; i < numWorkers; i++) {
			final ZMQStreamSocketReceiver receiver = new ZMQStreamSocketReceiver();

			RingBuffer<MessagePieceEvent> ringBuffer = RingBuffer.createSingleProducer(eventFactory, BUFFER_SIZE);
			BatchEventProcessor<MessagePieceEvent> processor = new BatchEventProcessor<>(ringBuffer,
					ringBuffer.newBarrier(), receiver);

			processor.setExceptionHandler(exceptionHandler);

			ringBuffers[i] = ringBuffer;
			threads.add(threadFactory.newThread(processor));
			barriers[i] = ringBuffer.newBarrier(processor.getSequence());
		}

		Runnable processor = handlerInitializer.initHandlers(ringBuffers, barriers);
		threads.add(threadFactory.newThread(processor));

		for (Thread thread : threads) {
			thread.start();
		}

		return new ZMQMessagePieceReceiver() {

			@Override
			public void shutdown() {
				for (Thread thread : threads) {
					thread.interrupt();
				}
			}

			private void publish(RingBuffer<MessagePieceEvent> ringBuffer, int id, byte[] data) {
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
			public void publish(int id, byte[] data) {
				RingBuffer<MessagePieceEvent> ringBuffer = ringBuffers[Math.abs(id % numWorkers)];
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

			final ZMQSocket socket = this.openSocket("tcp://*:8787", ZMQSocketType.SERVER);

			final ZMQSocketResponder responder = initResponder(socket);

			final ZMQMessagePieceReceiver receiver = this.initReceiver(4, new EventHandlerInitializer() {

				@Override
				public Runnable initHandlers(RingBuffer<MessagePieceEvent>[] ringBuffers, SequenceBarrier[] barriers) {
					MultiBufferBatchEventProcessor<MessagePieceEvent> processor = new MultiBufferBatchEventProcessor<MessagePieceEvent>(
							ringBuffers, barriers, new EventHandler<MessagePieceEvent>() {

								// private int count = 0;

								@Override
								public void onEvent(MessagePieceEvent event, long sequence, boolean endOfBatch)
										throws Exception {
									if (event.getExtractedMessages() != null) {
										// count += event.getExtractedMessages().size();
										// getLogger().debug("Received messages: {}, first one: {}", count,
										// event.getExtractedMessages().get(0));

										for (PuElement message : event.getExtractedMessages()) {
											// getLogger().debug("Sending response...");
											responder.send(event.getId(), message);
										}
									}
								}
							});

					for (int i = 0; i < ringBuffers.length; i++) {
						ringBuffers[i].addGatingSequences(processor.getSequences()[i]);
					}
					return processor;
				}
			});

			responder.start();

			while (!Thread.currentThread().isInterrupted()) {
				Msg msg = (Msg) socket.recvMsg(0);
				receiver.publish(msg.getRoutingId(), msg.getData());
			}

			socket.close();
			receiver.shutdown();
			responder.shutdown();

		}, "Receiving Thread");

		recvThread.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			recvThread.interrupt();
		}));
	}
}
