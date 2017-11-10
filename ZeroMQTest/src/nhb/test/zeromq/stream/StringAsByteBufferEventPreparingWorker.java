package nhb.test.zeromq.stream;

import java.nio.ByteBuffer;

import com.lmax.disruptor.WorkHandler;

public class StringAsByteBufferEventPreparingWorker implements WorkHandler<StringAsByteBufferEvent> {

	public static StringAsByteBufferEventPreparingWorker[] createHandlers(int size) {
		if (size > 0) {
			StringAsByteBufferEventPreparingWorker[] results = new StringAsByteBufferEventPreparingWorker[size];
			for (int i = 0; i < size; i++) {
				results[i] = new StringAsByteBufferEventPreparingWorker();
			}
			return results;
		}
		throw new IllegalArgumentException("Worker pool size cannot be negative or zero");
	}

	@Override
	public void onEvent(StringAsByteBufferEvent event) throws Exception {
		ByteBuffer buffer = event.getBuffer();
		buffer.putInt(event.getMessage().length());
		buffer.put(event.getMessage().getBytes());
	}
}