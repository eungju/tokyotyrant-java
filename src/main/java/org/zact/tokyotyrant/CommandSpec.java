package org.zact.tokyotyrant;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommandSpec {
	List<FieldSpec> fieldSpecs;
	
	public CommandSpec(String input) {
		fieldSpecs = new ArrayList<FieldSpec>();
		Pattern fieldPattern = Pattern.compile("\\[(.+?):(?:(\\d)|(.+?))\\]");
		Matcher matcher = fieldPattern.matcher(input);
		while (matcher.find()) {
			String name = matcher.group(1);
			FieldSpec fieldSpec = matcher.group(2) == null ? new FieldSpec(name, matcher.group(3)) : new FieldSpec(name, Integer.parseInt(matcher.group(2)));
			fieldSpecs.add(fieldSpec);
		}
	}
	
	public ByteBuffer encode(Map<String, Object> encoded) {
		int capacity = 0;
		for (FieldSpec each : fieldSpecs) {
			capacity += each.size(encoded);
		}
		ByteBuffer out = ByteBuffer.allocate(capacity);
		for (FieldSpec each : fieldSpecs) {
			Object value = encoded.get(each.name);
			System.out.println(value);
			if (value instanceof byte[]) {
				out.put((byte[])value);
			} else if (value instanceof Integer) {
				out.putInt((Integer)value);
			}
		}
		out.flip();
		return out;
	}

	public boolean decode(Map<String, Object> decoded, ByteBuffer in) {
		for (FieldSpec each : fieldSpecs) {
			int size = each.size(decoded);
			if (in.remaining() < size) {
				return false;
			}
			if (each.size == 1) {
				decoded.put(each.name, in.get());
			} else if (each.size == 4) {
				decoded.put(each.name, in.getInt());
			} else {
				byte[] buf = new byte[(Integer)decoded.get(each.sizeVariable)];
				in.get(buf);
				decoded.put(each.name, buf);
			}
		}
		return true;
	}

	public static FieldSpec field(String name, int size) {
		return new FieldSpec(name, size);
	}
	
	private static class FieldSpec extends ObjectSupport {
		public String name;
		public int size;
		public String sizeVariable = null;
		
		public FieldSpec(String name, int size) {
			this.name = name;
			this.size = size;
		}

		public FieldSpec(String name, String sizeVariable) {
			this.name = name;
			this.sizeVariable = sizeVariable;
		}
		
		public int size(Map<String, Object> context) {
			return sizeVariable == null ? size : (Integer)context.get(sizeVariable);
		}
	}
}
