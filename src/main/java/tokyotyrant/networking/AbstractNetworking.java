package tokyotyrant.networking;

import java.net.URI;

import tokyotyrant.protocol.Command;

public abstract class AbstractNetworking implements Networking {
	protected URI[] addresses;
	protected NodeLocator nodeLocator;
	protected NodeSelector nodeSelector;
	protected ReconnectQueue reconnectQueue = new ReconnectQueue();
	
	protected AbstractNetworking(NodeLocator nodeLocator) {
		this.nodeLocator = nodeLocator;
		this.nodeSelector = new NodeSelector(this.nodeLocator);
	}

	public void initialize(URI[] addresses) {
		this.addresses = addresses;
	}

	public void send(Command<?> command) {
		send(nodeSelector.select(), command);
	}
	
	public void send(ServerNode node, Command<?> command) {
		node.send(command);
	}
	
	public NodeLocator getNodeLocator() {
		return nodeLocator;
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
