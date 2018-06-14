package nhb.test.zeromq.stream;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;

import com.nhb.common.Loggable;
import com.nhb.common.data.PuElement;
import com.nhb.common.data.msgpkg.PuElementTemplate;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import lombok.Getter;

public class MessagePieceCumulator implements Loggable {

	private static final MessagePack msgpack = new MessagePack();

	private static final int INTEGER_SIZE = Integer.BYTES;

	@Getter
	private int status = 0;

	private final ByteBuf buffer;
	private int dataLength = 0;

	@Getter
	private long totalReceivedMessages = 0;

	private AtomicBoolean isChecking = new AtomicBoolean(false);

	public MessagePieceCumulator() {
		this.buffer = Unpooled.buffer(1024);
	}

	public long getRemaining() {
		return this.buffer.readableBytes();
	}

	public List<PuElement> receive(byte[] bytes) {
		if (bytes == null || bytes.length == 0) {
			return null;
		}
		// getLogger().debug("Adding {} bytes data", bytes.length);
		this.buffer.ensureWritable(bytes.length);
		this.buffer.writeBytes(bytes);
		// System.out.println("Added successful...");
		List<PuElement> extractedMessages = new LinkedList<>();
		this.checkData(extractedMessages);
		totalReceivedMessages += extractedMessages.size();
		return extractedMessages;
	}

	private void checkData(List<PuElement> extractedMessages) {
		if (isChecking.compareAndSet(false, true)) {
			AtomicInteger remainingTaskCount = new AtomicInteger(0);

			do {
				_continueChecking(remainingTaskCount, extractedMessages);
			} while (remainingTaskCount.decrementAndGet() >= 0);

			isChecking.set(false);
		}
	}

	private void _continueChecking(AtomicInteger remainingTaskCount, List<PuElement> output) {
		if (this.status == 0 && this.getRemaining() >= 4) {
			this.dataLength = buffer.readInt();
			if (this.dataLength <= 0) {
				getLogger().error("Data length cannot be zero negative", new Exception("Invalid data length value"));
				this.status = 0;
			} else {
				// System.out.println(
				// "--> waiting for data length: " + dataLength + ", current remaining: " +
				// this.getRemaining());
				this.status = 1;
				// getLogger().debug("Waiting for {} bytes data", dataLength);
			}
			remainingTaskCount.incrementAndGet();
		} else if (status == 1 && this.getRemaining() >= this.dataLength) {

			ByteBuf tmpBuf = this.buffer.slice(INTEGER_SIZE, dataLength);
			// getLogger().debug("data length enough: {}", tmpBuf.readableBytes());

			try (ByteBufInputStream is = new ByteBufInputStream(tmpBuf)) {
				Unpacker unpacker = msgpack.createUnpacker(is);
				while (is.available() > 0) {
					PuElement message = PuElementTemplate.getInstance().read(unpacker, null);
					if (message != null) {
						output.add(message);
					}
				}
			} catch (Exception e) {
				getLogger().error(
						"Error while deserialize bytes to message, dataLength: {}, readerIndex: {}, writerIndex: {}",
						dataLength, this.buffer.readerIndex(), this.buffer.writerIndex(), e);
			} finally {
				this.buffer.readerIndex(this.dataLength + INTEGER_SIZE);
				this.buffer.markWriterIndex();
				this.buffer.discardReadBytes();
				this.buffer.resetWriterIndex();
			}

			this.status = 0;
			remainingTaskCount.incrementAndGet();
		}
	}

}
