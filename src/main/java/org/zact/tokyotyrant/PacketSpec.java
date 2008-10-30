package org.zact.tokyotyrant;

import java.nio.ByteBuffer;
import java.util.Map;

public class PacketSpec {
	private FieldSpec[] fields;
	
	public PacketSpec(FieldSpec...fields) {
		this.fields = fields;
	}
	
	public ByteBuffer encode(Map<String, Object> context) {
		int capacity = 0;
		for (FieldSpec each : fields) {
			capacity += each.size(context);
		}
		ByteBuffer out = ByteBuffer.allocate(capacity);
		for (FieldSpec each : fields) {
			Object value = context.get(each.name);
			if (each.type.equals(byte[].class)) {
				out.put((byte[])value);
			} else if (each.type.equals(Integer.class)) {
				out.putInt((Integer)value);
			} else if (each.type.equals(Byte.class)) {
				out.put((Byte)value);
			} else if (each.type.equals(Long.class)) {
				out.putLong((Long)value);
			} else {
				throw new UnsupportedOperationException("Doesn't support type " + each.type);
			}
		}
		out.flip();
		return out;
	}

	public boolean decode(Map<String, Object> context, ByteBuffer in) {
		for (FieldSpec each : fields) {
			int size = each.size(context);
			if (in.remaining() < size) {
				return false;
			}

			if (each.type.equals(byte[].class)) {
				byte[] buf = new byte[each.size(context)];
				in.get(buf);
				context.put(each.name, buf);
			} else if (each.type.equals(Integer.class)) {
				context.put(each.name, in.getInt());
			} else if (each.type.equals(Byte.class)) {
				context.put(each.name, in.get());
			} else if (each.type.equals(Long.class)) {
				context.put(each.name, in.getLong());
			} else {
				throw new UnsupportedOperationException("Doesn't support type " + each.type);
			}
			
			if (!each.needMore(context)) {
				return true;
			}
		}
		return true;
	}

	public static PacketSpec packet(FieldSpec...fields) {
		return new PacketSpec(fields);
	}
	
	public static FieldSpec magic() {
		return bytes("magic", 2);
	}
	
	public static FieldSpec code(boolean stopWhenError) {
		return new CodeFieldSpec(stopWhenError);
	}
	
	public static FieldSpec int8(String name) {
		return new FieldSpec(name, Byte.class, 1);
	}

	public static FieldSpec int32(String name) {
		return new FieldSpec(name, Integer.class, 4);
	}

	public static FieldSpec int64(String name) {
		return new FieldSpec(name, Long.class, 8);
	}
	
	public static FieldSpec bytes(String name, int size) {
		return new FieldSpec(name, byte[].class, size);
	}

	public static FieldSpec bytes(String name, String sizeVariable) {
		return new FieldSpec(name, byte[].class, sizeVariable);
	}
	
	private static class FieldSpec extends ObjectSupport {
		public String name;
		public Class<?> type;
		public int size;
		public String sizeVariable = null;
		
		public FieldSpec(String name, Class<?> type, int size) {
			this.name = name;
			this.type = type;
			this.size = size;
		}

		public FieldSpec(String name, Class<?> type, String sizeVariable) {
			this.name = name;
			this.type = type;
			this.sizeVariable = sizeVariable;
		}
		
		public int size(Map<String, Object> context) {
			return sizeVariable == null ? size : (Integer)context.get(sizeVariable);
		}
		
		public boolean needMore(Map<String, Object> context) {
			return true;
		}
	}
	
	private static class CodeFieldSpec extends FieldSpec {
		private boolean stopWhenError;

		public CodeFieldSpec(boolean stopWhenError) {
			super("code", Byte.class, 1);
			this.stopWhenError = stopWhenError;
		}
		
		public boolean needMore(Map<String, Object> context) {
			return !stopWhenError || 0 == (Byte)context.get(name);
		}
	}
}
