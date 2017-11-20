package nhb.test.zeromq.stream.server;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;

public interface EventHandlerInitializer {

	Runnable initHandlers(RingBuffer<MessagePieceEvent>[] ringBuffers, SequenceBarrier[] barriers);
}
