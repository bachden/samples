package nhb.test.zeromq.stream.server;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.nhb.common.Loggable;
import com.nhb.common.data.PuElement;
import com.nhb.common.data.PuObject;
import com.nhb.common.data.msgpkg.PuElementTemplate;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import lombok.Getter;

public class MessagePieceCumulator implements Loggable {

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
		// System.out.println("Adding " + bytes.length + " bytes data");
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
				PuElement message = _continueChecking(remainingTaskCount);
				if (message != null) {
					extractedMessages.add(message);
				}
			} while (remainingTaskCount.decrementAndGet() >= 0);
			isChecking.set(false);
		}
	}

	private PuElement _continueChecking(AtomicInteger remainingTaskCount) {
		if (this.status == 0 && this.getRemaining() >= 4) {
			this.dataLength = buffer.readInt();
			if (this.dataLength < 0) {
				getLogger().error("Data length cannot be negative", new Exception("Invalid data length value"));
			} else if (this.dataLength == 0) {
				this.status = 0;
				// new Exception("Got dataLength == 0").printStackTrace();
			} else {
				// System.out.println(
				// "--> waiting for data length: " + dataLength + ", current remaining: " +
				// this.getRemaining());
				this.status = 1;
				// getLogger().debug("Waiting for {} bytes data", dataLength);
			}
			remainingTaskCount.incrementAndGet();
		} else if (status == 1 && this.getRemaining() >= this.dataLength) {

			ByteBuf tmpBuf = this.buffer.slice(INTEGER_SIZE, dataLength + INTEGER_SIZE);
			PuElement message = null;
			try (ByteBufInputStream is = new ByteBufInputStream(tmpBuf)) {
				message = PuElementTemplate.getInstance().read(is);
				if (!(message instanceof PuObject)) {
					getLogger().warn("Message is not a puobject, type: {}, bytes length: {}", message.getClass(),
							dataLength);
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

			return message;
		}
		return null;
	}

}
