package nhb.test.zeromq;

import com.nhb.common.data.PuObject;

public class TestPuElementToBytes {

	public static void main(String[] args) {
		PuObject msg = new PuObject();
		msg.setBoolean("boolVal", true);
		msg.setByte("byteVal", Byte.MAX_VALUE);
		msg.setShort("shortVal", Short.MAX_VALUE);
		msg.setInteger("intVal", Integer.MAX_VALUE);
		msg.setLong("longVal", Long.MAX_VALUE);
		msg.setFloat("floatVal", Float.MAX_VALUE);
		msg.setDouble("doubleVal", Double.MAX_VALUE);
		msg.setString("stringVal", "Nguyễn Hoàng Bách");
		
		
	}

}
