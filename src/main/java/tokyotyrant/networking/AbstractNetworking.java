package tokyotyrant.networking;

import tokyotyrant.protocol.Command;

public abstract class AbstractNetworking implements Networking {
	protected ReconnectQueue reconnectQueue = new ReconnectQueue();
	
	protected abstract TokyoTyrantNode[] getNodes();
	
	public void send(Command<?> command) {
		selectNode().send(command);
	}
	
	protected TokyoTyrantNode selectNode() {
		TokyoTyrantNode selected = getPrimaryNode();
		if (selected.isActive()) {
			return selected;
		}
		for (TokyoTyrantNode each : getNodes()) {
			if (each.isActive()) {
				selected = each;
				break;
			}
		}
		return selected;
	}
	
	protected TokyoTyrantNode getPrimaryNode() {
		return getNodes()[0];
	}
	
	protected void connectAllNodes() {
		for (TokyoTyrantNode each : getNodes()) {
			if (!each.connect()) {
				reconnectQueue.push(each);
			}
		}
	}
	
	protected void disconnectAllNodes() {
		for (TokyoTyrantNode each : getNodes()) {
			each.disconnect();
		}
	}
}
