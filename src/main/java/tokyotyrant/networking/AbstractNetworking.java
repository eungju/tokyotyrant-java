package tokyotyrant.networking;

import java.net.URI;

import tokyotyrant.protocol.Command;

public abstract class AbstractNetworking implements Networking {
	protected URI[] addresses;
	protected NodeLocator nodeLocator;
	protected NodeSelector nodeSelector;
	protected ReconnectionPolicy reconnectionPolicy = new ReconnectionPolicy();
	
	protected AbstractNetworking(NodeLocator nodeLocator, NodeSelector nodeSelector) {
		this.nodeLocator = nodeLocator;
		this.nodeSelector = nodeSelector;
	}

	public void initialize(URI[] addresses) {
		this.addresses = addresses;
	}

	public void send(Command<?> command) {
		send(nodeSelector.select(nodeLocator.getSequence()), command);
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
				reconnectionPolicy.reconnect(each);
			}
		}
	}
	
	protected void disconnectAllNodes() {
		for (ServerNode each : nodeLocator.getAll()) {
			each.disconnect();
		}
	}
}
