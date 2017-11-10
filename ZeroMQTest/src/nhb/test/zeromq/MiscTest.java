package nhb.test.zeromq;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ByteBufferInput;

public class MiscTest {

	public static void main(String[] args) throws IOException {
		MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

		packer.packArrayHeader(3);

		packer.packInt(1);
		packer.packString("Nguyễn Hoàng Bách");

		packer.packMapHeader(3);
		packer.packString("first");
		packer.packString("Bách");
		packer.packString("middle");
		packer.packString("Hoàng");
		packer.packString("last");
		packer.packString("Nguyễn");

		packer.packArrayHeader(3);

		packer.packInt(1);
		packer.packString("Vũ Trọng Quý");

		packer.packMapHeader(3);
		packer.packString("first");
		packer.packString("Quý");
		packer.packString("middle");
		packer.packString("Trọng");
		packer.packString("last");
		packer.packString("Vũ");

		packer.close();

		byte[] bytes = packer.toByteArray();

		System.out.println("bytes as String: " + new String(bytes));
		System.out.println("bytes: " + Arrays.toString(bytes));

		int middle = bytes.length / 2;
		byte[] bytes1 = Arrays.copyOfRange(bytes, 0, middle);
		byte[] bytes2 = Arrays.copyOfRange(bytes, middle, bytes.length);

		doUnpack(bytes1, bytes2);
		// System.out.println("total length: " + bytes.length + ", piece1 length: " +
		// bytes1.length + ", pieces2 length: "
		// + bytes2.length);
	}

	private static void doUnpack(byte[]... pieces) throws IOException {
		if (pieces != null && pieces.length > 0) {
			List<byte[]> list = new ArrayList<>();
			for (byte[] piece : pieces) {
				list.add(piece);
			}
			MessageUnpacker unpacker = null;
			while (list.size() > 0) {
				byte[] bytes = list.remove(0);
				if (unpacker == null) {
					unpacker = MessagePack.newDefaultUnpacker(bytes);
				} else {
					unpacker.reset(new ByteBufferInput(ByteBuffer.wrap(bytes)));
				}
				try {
					_unpack(unpacker);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static void _unpack(MessageUnpacker unpacker) throws IOException {
		while (unpacker.hasNext()) {
			MessageFormat nextFormat = unpacker.getNextFormat();
			switch (nextFormat.getValueType()) {
			case ARRAY:
				System.out.println("found array with size: " + unpacker.unpackArrayHeader());
				break;
			case MAP:
				System.out.println("found map with size: " + unpacker.unpackMapHeader());
				break;
			case INTEGER:
				System.out.println("found integer: " + unpacker.unpackInt());
				break;
			case STRING:
				System.out.println("found string: " + unpacker.unpackString());
				break;
			default:
				System.out.println("found message type: " + nextFormat);
				break;
			}
		}
	}
}
