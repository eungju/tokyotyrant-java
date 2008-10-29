package org.zact.tokyotyrant;

import java.nio.ByteBuffer;
import java.util.Map;

public class CommandSpec {
	private FieldSpec[] fieldSpecs;
	
	public CommandSpec(FieldSpec...fields) {
		this.fieldSpecs = fields;
	}
	
	public ByteBuffer encode(Map<String, Object> context) {
		int capacity = 0;
		for (FieldSpec each : fieldSpecs) {
			capacity += each.size(context);
		}
		ByteBuffer out = ByteBuffer.allocate(capacity);
		for (FieldSpec each : fieldSpecs) {
			Object value = context.get(each.name);
			if (each.type.equals(byte[].class)) {
				out.put((byte[])value);
			} else if (each.type.equals(Integer.class)) {
				out.putInt((Integer)value);
			} else if (each.type.equals(Byte.class)) {
				out.put((Byte)value);
			} else if (each.type.equals(Long.class)) {
				out.putLong((Long)value);
			} else if (each.type.equals(String.class)) {
				out.put(((String)value).getBytes());
			} else {
				new UnsupportedOperationException("Doesn't support type " + each.type);
			}
		}
		out.flip();
		return out;
	}

	public boolean decode(Map<String, Object> context, ByteBuffer in) {
		for (FieldSpec each : fieldSpecs) {
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
			} else if (each.type.equals(String.class)) {
				byte[] buf = new byte[each.size(context)];
				in.get(buf);
				context.put(each.name, new String(buf));
			} else {
				new UnsupportedOperationException("Doesn't support type " + each.type);
			}
			
			if (!each.needMore(context)) {
				return true;
			}
		}
		return true;
	}

	public static CommandSpec packet(FieldSpec...fields) {
		return new CommandSpec(fields);
	}
	
	public static FieldSpec field(String name, Class<?> type, int size) {
		return new FieldSpec(name, type, size);
	}

	public static FieldSpec field(String name, Class<?> type, String sizeVariable) {
		return new FieldSpec(name, type, sizeVariable);
	}
	
	public static FieldSpec magic() {
		return field("magic", byte[].class, 2);
	}
	
	public static FieldSpec code(boolean proceedAlways) {
		return new CodeFieldSpec(proceedAlways);
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
		private boolean proceedAlways;

		public CodeFieldSpec(boolean proceedAlways) {
			super("code", Byte.class, 1);
			this.proceedAlways = proceedAlways;
		}
		
		public boolean needMore(Map<String, Object> context) {
			return proceedAlways || 0 == (Byte)context.get(name);
		}
	}
}
