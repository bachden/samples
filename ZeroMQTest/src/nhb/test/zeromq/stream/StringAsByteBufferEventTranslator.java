package nhb.test.zeromq.stream;

import com.lmax.disruptor.EventTranslatorOneArg;

public class StringAsByteBufferEventTranslator implements EventTranslatorOneArg<StringAsByteBufferEvent, String> {

	public static final StringAsByteBufferEventTranslator DEFAULT = new StringAsByteBufferEventTranslator();

	@Override
	public void translateTo(StringAsByteBufferEvent event, long sequence, String message) {
		event.reset(sequence, message);
	}
}