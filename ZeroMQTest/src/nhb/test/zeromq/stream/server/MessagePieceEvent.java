package nhb.test.zeromq.stream.server;

import java.util.List;

import com.lmax.disruptor.EventFactory;
import com.nhb.common.data.PuElement;
import com.nhb.common.vo.ByteArrayWrapper;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

public class MessagePieceEvent {

	@Setter
	@Getter
	private long sequence;
	
	@Getter
	@Setter(AccessLevel.PACKAGE)
	private ByteArrayWrapper id;

	@Getter
	@Setter(AccessLevel.PACKAGE)
	private byte[] data;

	@Getter
	@Setter(AccessLevel.PACKAGE)
	private int readBytes;

	@Setter
	@Getter
	private List<PuElement> extractedMessages;

	private MessagePieceEvent() {

	}

	public static final EventFactory<MessagePieceEvent> newFactory() {

		return new EventFactory<MessagePieceEvent>() {

			@Override
			public MessagePieceEvent newInstance() {
				return new MessagePieceEvent();
			}
		};
	}
}
