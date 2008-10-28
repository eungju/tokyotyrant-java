package org.zact.tokyotyrant;

import java.net.InetSocketAddress;
import java.util.concurrent.Future;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

public class TyrantClient {
	private long timeout = 1000L;
	private NioSocketConnector connector;
	private IoSession session;
    
    public TyrantClient(String host, int port) {
        connector = new NioSocketConnector();
        connector.setConnectTimeoutMillis(timeout);
        connector.getFilterChain().addLast("logger", new LoggingFilter());
        connector.getFilterChain().addLast("tyrant", new ProtocolCodecFilter(new TyrantProtocolCodecFactory()));
        connector.setHandler(new TyrantProtocolHandler());
        connect(new InetSocketAddress(host, port));
	}

    public void connect(InetSocketAddress remoteAddress) {
        ConnectFuture cf = connector.connect(remoteAddress);
        if (cf.awaitUninterruptibly(timeout)) {
        	session = cf.getSession();
        	return;
        }
        throw new RuntimeException("Cannot connect to the server " + remoteAddress);
    }
    
	public void close() {
        session.close();
	}
	
	public void dispose() {
		connector.dispose();
	}
	
	public Future<Boolean> put(String key, String value) {
		Put command = new Put(key.getBytes(), value.getBytes());
		session.write(command);
		return command.getFuture();
	}

	public Future<Boolean> out(String key) {
		Out command = new Out(key.getBytes());
		session.write(command);
		return command.getFuture();
	}
	
	public Future<Object> get(String key) {
		Get command = new Get(key.getBytes());
		session.write(command);
		return command.getFuture();
	}
}
