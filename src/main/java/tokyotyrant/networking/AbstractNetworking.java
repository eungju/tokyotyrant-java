package tokyotyrant.networking;

import java.net.URI;
import java.util.Iterator;

import tokyotyrant.protocol.Command;

public abstract class AbstractNetworking implements Networking {
	protected URI[] addresses;
	protected NodeLocator nodeLocator;
	protected ReconnectQueue reconnectQueue;
	
	protected AbstractNetworking(NodeLocator nodeLocator) {
		this.nodeLocator = nodeLocator;
		this.reconnectQueue = new ReconnectQueue();
	}
	
	public void initialize(URI[] addresses) {
		this.addresses = addresses;
	}

	public void send(Command<?> command) {
		send(selectNode(), command);
	}
	
	public void send(ServerNode node, Command<?> command) {
		node.send(command);
	}
	
	public NodeLocator getNodeLocator() {
		return nodeLocator;
	}
	
	protected ServerNode selectNode() {
		Iterator<ServerNode> backups = nodeLocator.getSequence();
		ServerNode selected = backups.next();
		if (selected.isActive()) {
			return selected;
		}
		while (backups.hasNext()) {
			ServerNode each = backups.next();
			if (each.isActive()) {
				selected = each;
				break;
			}
		}
		return selected;
	}
	
	protected void connectAllNodes() {
		for (ServerNode each : nodeLocator.getAll()) {
			if (!each.connect()) {
				reconnectQueue.push(each);
			}
		}
	}
	
	protected void disconnectAllNodes() {
		for (ServerNode each : nodeLocator.getAll()) {
			each.disconnect();
		}
	}
}
