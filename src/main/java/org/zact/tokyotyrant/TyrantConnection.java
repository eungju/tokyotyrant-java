package org.zact.tokyotyrant;

import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.session.AttributeKey;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

public class TyrantConnection {
	private long timeout = 1000L;
	private NioSocketConnector connector;
	private IoSession session;
	private LinkedBlockingQueue<Command> queue = new LinkedBlockingQueue<Command>();
    
    public TyrantConnection(String host, int port) {
        connector = new NioSocketConnector();
        connector.setConnectTimeoutMillis(timeout);
        connector.getFilterChain().addLast("logger", new LoggingFilter());
        connector.getFilterChain().addLast("tyrant", new ProtocolCodecFilter(new TyrantProtocolCodecFactory()));
        connector.setHandler(new TyrantProtocolHandler(queue));
        ConnectFuture cf = connector.connect(new InetSocketAddress(host, port));
        cf.awaitUninterruptibly(timeout);
        session = cf.getSession();
	}
    
	public void close() {
        session.close().awaitUninterruptibly(timeout);
		connector.dispose();
	}

	public static final AttributeKey COMMAND_KEY = new AttributeKey(TyrantConnection.class, "CurrentCommand");
	
	public boolean put(String key, String value) {
		Put command = new Put(key, value);
		session.setAttribute(COMMAND_KEY, command);
		session.write(command);
		try {
			command = (Put)queue.poll(timeout, TimeUnit.MILLISECONDS);
			return command.isSuccess();
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted");
		} finally {
			session.removeAttribute(TyrantConnection.COMMAND_KEY);
		}
	}

	public boolean out(String key) {
		Out command = new Out(key);
		session.setAttribute(COMMAND_KEY, command);
		session.write(command);
		try {
			command = (Out)queue.poll(timeout, TimeUnit.MILLISECONDS);
			return command.isSuccess();
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted");
		} finally {
			session.removeAttribute(TyrantConnection.COMMAND_KEY);
		}
	}
	
	public String get(String key) {
		Get command = new Get(key);
		session.setAttribute(COMMAND_KEY, command);
		session.write(command);
		try {
			command = (Get)queue.poll(timeout, TimeUnit.MILLISECONDS);
			return command.isSuccess() ? command.getValue() : null;
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted");
		} finally {
			session.removeAttribute(TyrantConnection.COMMAND_KEY);
		}
	}
}
