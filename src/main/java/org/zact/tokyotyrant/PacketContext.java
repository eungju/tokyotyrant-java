package org.zact.tokyotyrant;

import java.util.HashMap;
import java.util.Map;

public final class PacketContext {
	private final Map<String, Object> fields;
	
	public PacketContext() {
		fields = new HashMap<String, Object>();		
	}
	
	public PacketContext(int numberOfFields) {
		fields = new HashMap<String, Object>(numberOfFields);		
	}
	
	public void put(String key, Object value) {
		fields.put(key, value);
	}
	
	public Object get(String key) {
		return fields.get(key);
	}
}