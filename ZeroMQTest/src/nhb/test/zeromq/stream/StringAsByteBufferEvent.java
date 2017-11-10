package nhb.test.zeromq.stream;

import java.nio.ByteBuffer;

import com.lmax.disruptor.EventFactory;

import lombok.Getter;

public class StringAsByteBufferEvent {

	private final int mask;
	private final int exponent;
	private final int entrySize;
	private final ByteBuffer sharedByteBuffer;

	@Getter
	private String message;

	@Getter
	private ByteBuffer buffer;

	private int index(long sequence) {
		return (int) (sequence & mask);
	}

	public int size() {
		return Integer.BYTES + message.length();
	}

	public void reset(long sequence, String message) {
		int index = index(sequence);
		int position = index << this.exponent;
		int limit = position + entrySize;

		// System.out.println("Index: " + index + ", offset: " + position + ", limit:" +
		// limit + ", message length: "
		// + message.length());

		ByteBuffer byteBuffer = this.sharedByteBuffer.duplicate();
		byteBuffer.position(position).limit(limit);

		this.buffer = byteBuffer;
		this.message = message;
	}

	private StringAsByteBufferEvent(ByteBuffer sharedByteBuffer, int entrySize, int mask) {
		this.mask = mask;
		this.entrySize = entrySize;
		this.sharedByteBuffer = sharedByteBuffer;
		this.exponent = (int) (Math.log(entrySize) / Math.log(2));
	}

	public static final EventFactory<StringAsByteBufferEvent> newFactory(final int bufferSize, final int entrySize) {

		if (entrySize <= 0 || Integer.bitCount(entrySize) != 1) {
			throw new IllegalArgumentException("Entry size must be power by 2");
		}

		final int mask = bufferSize - 1;
		final ByteBuffer sharedByteBuffer = ByteBuffer.allocateDirect(bufferSize * entrySize);

		return new EventFactory<StringAsByteBufferEvent>() {

			@Override
			public StringAsByteBufferEvent newInstance() {
				return new StringAsByteBufferEvent(sharedByteBuffer, entrySize, mask);
			}
		};
	}
}