package nhb.test.zeromq.utils;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

import sun.misc.Unsafe;

public class UnsafeUtils {

	private static final Unsafe THE_UNSAFE;

	private static final long STRING_VALUE_FIELD_OFFSET;

	static {
		try {
			final PrivilegedExceptionAction<Unsafe> action = new PrivilegedExceptionAction<Unsafe>() {
				public Unsafe run() throws Exception {
					Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
					theUnsafe.setAccessible(true);
					return (Unsafe) theUnsafe.get(null);
				}
			};

			THE_UNSAFE = AccessController.doPrivileged(action);
			STRING_VALUE_FIELD_OFFSET = THE_UNSAFE.objectFieldOffset(String.class.getDeclaredField("value"));
		} catch (Exception e) {
			throw new RuntimeException("Unable to load unsafe", e);
		}
	}

	public static final Unsafe getUnsafe() {
		return THE_UNSAFE;
	}

	public static char[] getStringValue(String str) {
		return (char[]) THE_UNSAFE.getObject(str, STRING_VALUE_FIELD_OFFSET);
	}
}
