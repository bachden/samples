package nhb.test.zeromq.stream.client;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.nhb.common.data.PuElement;

import lombok.Getter;
import lombok.Setter;

public class MessageBufferEvent {

	@Getter
	private long sequence;

	@Getter
	@Setter
	private byte[] socketId;

	@Getter
	@Setter
	private PuElement message;

	private MessageBufferEvent() {
	}

	public static final EventFactory<MessageBufferEvent> newFactory() {

		return new EventFactory<MessageBufferEvent>() {

			@Override
			public MessageBufferEvent newInstance() {
				return new MessageBufferEvent();
			}
		};
	}

	public static final EventTranslatorOneArg<MessageBufferEvent, PuElement> TRANSLATOR = new EventTranslatorOneArg<MessageBufferEvent, PuElement>() {

		@Override
		public void translateTo(MessageBufferEvent event, long sequence, PuElement message) {
			event.message = message;
			event.sequence = sequence;
		}
	};
}