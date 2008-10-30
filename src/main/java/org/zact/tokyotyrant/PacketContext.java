package org.zact.tokyotyrant;

import java.util.HashMap;
import java.util.Map;

public class PacketContext {
	private Map<String, Object> properties = new HashMap<String, Object>();
	public void put(String key, Object value) {
		properties.put(key, value);
	}
	public Object get(String key) {
		return properties.get(key);
	}
}