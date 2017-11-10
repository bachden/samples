package nhb.test.zeromq.union;

import java.util.Arrays;

import javolution.io.Union;
import lombok.Getter;

@Getter
public class UnionNumber extends Union {

	private Signed8 asByte = new Signed8();
	private Signed16 asShort = new Signed16();
	private Signed32 asInt = new Signed32();
	private Signed64 asLong = new Signed64();

	private Float32 asFloat = new Float32();
	private Float64 asDouble = new Float64();

	public static void main(String[] args) {
		UnionNumber num = new UnionNumber();
		num.getAsInt().set(10);
		System.out.println("int value: " + num.getAsFloat().get());
		System.out.println("byte array: " + Arrays.toString(num.getByteBuffer().array()));
	}
}
