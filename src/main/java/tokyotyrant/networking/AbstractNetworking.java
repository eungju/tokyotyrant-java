package tokyotyrant.networking;

import tokyotyrant.protocol.Command;

public abstract class AbstractNetworking implements Networking {
	protected NodeLocator nodeLocator;
	protected ReconnectQueue reconnectQueue;
	
	protected AbstractNetworking(NodeLocator nodeLocator) {
		this.nodeLocator = nodeLocator;
		this.reconnectQueue = new ReconnectQueue();
	}
	
	public void send(Command<?> command) {
		send(selectNode(), command);
	}
	
	public void send(TokyoTyrantNode node, Command<?> command) {
		node.send(command);
	}
	
	protected TokyoTyrantNode selectNode() {
		TokyoTyrantNode selected = nodeLocator.getPrimary();
		if (selected.isActive()) {
			return selected;
		}
		for (TokyoTyrantNode each : nodeLocator.getAll()) {
			if (each.isActive()) {
				selected = each;
				break;
			}
		}
		return selected;
	}
	
	protected void connectAllNodes() {
		for (TokyoTyrantNode each : nodeLocator.getAll()) {
			if (!each.connect()) {
				reconnectQueue.push(each);
			}
		}
	}
	
	protected void disconnectAllNodes() {
		for (TokyoTyrantNode each : nodeLocator.getAll()) {
			each.disconnect();
		}
	}
}
