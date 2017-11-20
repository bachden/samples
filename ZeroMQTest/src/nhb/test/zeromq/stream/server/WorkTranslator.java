package nhb.test.zeromq.stream.server;

import com.lmax.disruptor.WorkHandler;

public interface WorkTranslator<F, T> extends WorkHandler<F> {

	@Override
	default void onEvent(F event) throws Exception {
		this.processEvent(this.translate(event));
	}

	T translate(F inputEvent);

	void processEvent(T event);
}
