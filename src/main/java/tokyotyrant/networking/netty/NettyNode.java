package tokyotyrant.networking.netty;

import static org.jboss.netty.channel.Channels.*;

import java.net.SocketAddress;
import java.net.URI;
import java.util.Map;
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

import tokyotyrant.helper.UriHelper;
import tokyotyrant.networking.ServerNode;
import tokyotyrant.protocol.Command;

@ChannelPipelineCoverage("one")
public class NettyNode extends FrameDecoder implements ServerNode {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private URI address;
	private SocketAddress socketAddress;
	private Map<String, String> parameters;

	private NettyNetworking networking;
	private ClientBootstrap bootstrap;
	private Channel channel;
	private int reconnecting;
	private BlockingQueue<Command<?>> readingCommands = new LinkedBlockingQueue<Command<?>>();

	public NettyNode(NettyNetworking networking) {
		this.networking = networking;
	}
	
	public void initialize(URI address) {
		if (!"tcp".equals(address.getScheme())) {
			throw new IllegalArgumentException("Only support Tokyo Tyrant protocol");
		}
		this.address = address;
		
		socketAddress = UriHelper.getSocketAddress(address);
		parameters = UriHelper.getParameters(address);

		bootstrap = new ClientBootstrap(networking.getFactory());
		bootstrap.getPipeline().addLast("handler", this);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);
	}
	
	public URI getAddress() {
		return address;
	}
	
	public boolean isReadOnly() {
		return parameters.containsKey("readOnly") && "true".equals(parameters.get("readOnly"));
	}

	public boolean connect() {
		channel = bootstrap.connect(socketAddress).getChannel();
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
		networking.reconnect(this);
	}

	public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		Command<?> command = (Command<?>) e.getMessage();

		try {
			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
			command.encode(buffer);
			write(ctx, e.getChannel(), e.getFuture(), buffer, e.getRemoteAddress());
			command.reading();
			readingCommands.add(command);
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
