package tokyotyrant.networking.netty;

import java.util.concurrent.Executors;

import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import tokyotyrant.networking.AbstractNetworking;
import tokyotyrant.networking.NodeLocator;
import tokyotyrant.networking.NodeSelector;
import tokyotyrant.networking.ServerNode;

public class NettyNetworking extends AbstractNetworking {
	private ChannelFactory factory;

	public NettyNetworking(NodeLocator nodeLocator, NodeSelector nodeSelector) {
		super(nodeLocator, nodeSelector);
		factory = new NioClientSocketChannelFactory(Executors
				.newCachedThreadPool(), Executors.newCachedThreadPool());
	}
	
	public void start() throws Exception {
		NettyNode[] nodes = new NettyNode[addresses.length];
		for (int i = 0; i < addresses.length; i++) {
			nodes[i] = new NettyNode(this);
			nodes[i].initialize(addresses[i]);
		}
		nodeLocator.initialize(nodes);
		connectAllNodes();
		reconnections.start();
	}
	
	public void stop() {
		reconnections.stop();
		disconnectAllNodes();
		factory.releaseExternalResources();
	}
	
	public ChannelFactory getFactory() {
		return factory;
	}
	
	public void reconnect(ServerNode node) {
		reconnections.reconnect(node);
	}
}
