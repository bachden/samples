package nhb.test.zeromq.stream.server;

import com.lmax.disruptor.EventHandler;

public interface EventHandlerTranslator<F, T> extends EventHandler<F> {

	@Override
	default void onEvent(F event, long sequence, boolean endOfBatch) throws Exception {
		this.process(this.translate(event, sequence, endOfBatch));
	}

	T translate(F inputEvent, long sequence, boolean endOfBatch);

	void process(T event);
}
