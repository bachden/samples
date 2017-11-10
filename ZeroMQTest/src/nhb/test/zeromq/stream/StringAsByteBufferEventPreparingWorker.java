package nhb.test.zeromq.stream;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import com.lmax.disruptor.WorkHandler;

import nhb.test.zeromq.utils.UnsafeUtils;

public class StringAsByteBufferEventPreparingWorker implements WorkHandler<StringAsByteBufferEvent> {

	private final CharsetEncoder encoder = Charset.defaultCharset().newEncoder();

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

		char[] chars = UnsafeUtils.getStringValue(event.getMessage());
		CharBuffer cb = CharBuffer.wrap(chars);
		encoder.encode(cb, buffer, true);
	}
}