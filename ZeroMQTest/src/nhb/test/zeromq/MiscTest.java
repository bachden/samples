package nhb.test.zeromq;

import java.nio.ByteBuffer;

public class MiscTest {

	public static void main(String[] args) {
		byte[] bytes = new byte[] { -128, 0, 65, -85 };
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		System.out.println(byteBuffer.getInt());
	}
}
