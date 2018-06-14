package nhb.test.zeromq;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;

import com.nhb.common.utils.TimeWatcher;

public class TestStringGetBytes {

	private static byte[] getBytesFast(String str) {
		final int length = str.length();
		final char buffer[] = str.toCharArray();
		final byte b[] = new byte[length];
		for (int j = 0; j < length; j++)
			b[j] = (byte) buffer[j];
		return b;
	}

	public static void main(String[] args) {
		DecimalFormat df = new DecimalFormat("###,###.##");
		TimeWatcher timeWatcher = new TimeWatcher();
		final int msgSize = 1024;

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < msgSize; i++) {
			sb.append("a");
		}

		String msg = sb.toString();

		ByteBuffer byteBuffer = ByteBuffer.allocateDirect(msgSize);
		timeWatcher.reset();
		for (int i = 0; i < 1e6; i++) {
			byteBuffer.clear();
			byteBuffer.put(getBytesFast(msg));
		}
		System.out.println("Time to copy data to byte buffer using getBytesFast: "
				+ df.format(timeWatcher.endLapMillis()) + " milliseconds");

		timeWatcher.reset();
		for (int i = 0; i < 1e6; i++) {
			byteBuffer.clear();
			byteBuffer.put(msg.getBytes());
		}
		System.out.println("Time to copy data to byte buffer using native getBytes: "
				+ df.format(timeWatcher.endLapMillis()) + " milliseconds");
	}
}
