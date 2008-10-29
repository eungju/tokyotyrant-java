package org.zact.tokyotyrant;

import java.util.HashMap;
import java.util.Map;

public abstract class EasyCommand extends Command {
	public EasyCommand(byte commandId) {
		super(commandId);
	}

	protected Map<String, Object> context() {
		Map<String, Object> context = new HashMap<String, Object>();
		context.put("magic", magic);
		return context;
	}
}
