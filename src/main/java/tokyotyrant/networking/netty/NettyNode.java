package tokyotyrant.networking.netty;

import static org.jboss.netty.channel.Channels.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyotyrant.networking.NodeAddress;
import tokyotyrant.networking.ServerNode;
import tokyotyrant.protocol.Command;

@ChannelPipelineCoverage("one")
public class NettyNode extends FrameDecoder implements ServerNode {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private NodeAddress address;

	private NettyNetworking networking;
	private ClientBootstrap bootstrap;
	private Channel channel;
	private int reconnecting;
	private BlockingQueue<Command<?>> readingCommands = new LinkedBlockingQueue<Command<?>>();

	public NettyNode(NettyNetworking networking) {
		this.networking = networking;
	}
	
	public void initialize(NodeAddress address) {
		this.address = address;
		bootstrap = networking.getBootstrap(this);
	}
	
	public NodeAddress getAddress() {
		return address;
	}

	public boolean connect() {
		channel = bootstrap.connect(address.socketAddress()).getChannel();
		return true;
	}

	public void disconnect() {
		channel.close();
		readingCommands.clear();
	}

	public int getReconnectAttempt() {
		return reconnecting;
	}

	public boolean isActive() {
		return reconnecting == 0 && channel != null && channel.isConnected();
	}

	public void reconnecting() {
		logger.info("Reconnecting " + address);
		reconnecting++;
	}

	public void send(Command<?> command) {
		channel.write(command);
	}

    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
    	reconnecting = 0;
    }
    
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		logger.warn("Unexpected exception from downstream.", e.getCause());
		networking.getReconnectionMonitor().reconnect(this);
	}

	public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		Command<?> command = (Command<?>) e.getMessage();

		try {
			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
			command.encode(buffer);
			write(ctx, e.getChannel(), e.getFuture(), buffer, e.getRemoteAddress());
			command.reading();
			if (command.responseRequired()) {
				readingCommands.add(command);
			} else {
				command.complete();
			}
		} catch (Exception exception) {
			command.error(exception);
			throw new Exception("Error while sending " + command, exception);
		}
	}

	protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
		Command<?> command = readingCommands.peek();
		if (command == null) {
			return null;
		} 

		try {
			buffer.markReaderIndex();
			if (!command.decode(buffer)) {
				buffer.resetReaderIndex();
				return null;
			}
			command.complete();
			Command<?> _removed = readingCommands.remove();
			assert _removed == command;
			return command;
		} catch (Exception exception) {
			command.error(exception);
			throw new Exception("Error while receiving " + command, exception);
		}
	}
}
