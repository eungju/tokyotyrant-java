package tokyotyrant.command;

import java.nio.ByteBuffer;

public class PacketFormat {
	private Field[] fields;
	
	public PacketFormat(Field... fields) {
		this.fields = fields;
	}

	public ByteBuffer encode(PacketContext context) {
		int capacity = 0;
		for (Field each : fields) {
			capacity += each.size(context);
		}
		ByteBuffer out = ByteBuffer.allocate(capacity);
		for (Field each : fields) {
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

	public boolean decode(PacketContext context, ByteBuffer in) {
		for (Field each : fields) {
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
			
			if (!each.isExpectingMoreData(context)) {
				return true;
			}
		}
		return true;
	}

	static class Field {
		public String name;
		public Class<?> type;
		public int size;
		public String sizeVariable = null;
		
		public Field(String name, Class<?> type, int size) {
			this.name = name;
			this.type = type;
			this.size = size;
		}

		public Field(String name, Class<?> type, String sizeVariable) {
			this.name = name;
			this.type = type;
			this.sizeVariable = sizeVariable;
		}
		
		public int size(PacketContext context) {
			return sizeVariable == null ? size : (Integer)context.get(sizeVariable);
		}
		
		public boolean isExpectingMoreData(PacketContext context) {
			return true;
		}
	}
	
	static class CodeField extends Field {
		private boolean stopWhenError;

		public CodeField(boolean stopWhenError) {
			super("code", Byte.class, 1);
			this.stopWhenError = stopWhenError;
		}
		
		public boolean isExpectingMoreData(PacketContext context) {
			return 0 == (Byte)context.get(name) || !stopWhenError;
		}
	}
}
