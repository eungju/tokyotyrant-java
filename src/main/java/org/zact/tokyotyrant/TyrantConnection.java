package org.zact.tokyotyrant;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.mina.core.future.ConnectFuture;
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
        connector.getFilterChain().addLast("tyrant", new ProtocolCodecFilter(new TyrantProtocolCodecFactory(queue)));
        connector.setHandler(new TyrantProtocolHandler());
        ConnectFuture cf = connector.connect(new InetSocketAddress(host, port));
        cf.awaitUninterruptibly(timeout);
        session = cf.getSession();
	}
    
	public void close() {
        session.close().awaitUninterruptibly(timeout);
		connector.dispose();
	}
	
	public Future<Boolean> put(String key, String value) {
		final CountDownLatch latch = new CountDownLatch(1);
		final Put command = new Put(latch, key.getBytes(), value.getBytes());
    	queue.add(command);
		session.write(command);
		return new Future<Boolean>() {
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public Boolean get() throws InterruptedException,
					ExecutionException {
				latch.await();
				return command.isSuccess();
			}

			@Override
			public Boolean get(long timeout, TimeUnit unit)
					throws InterruptedException, ExecutionException,
					TimeoutException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public boolean isCancelled() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public boolean isDone() {
				// TODO Auto-generated method stub
				return false;
			}
		};
	}

	public Future<Boolean> out(String key) {
		final CountDownLatch latch = new CountDownLatch(1);
		final Out command = new Out(latch, key.getBytes());
    	queue.add(command);
		session.write(command);
		return new Future<Boolean>() {
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public Boolean get() throws InterruptedException,
					ExecutionException {
				latch.await();
				return command.isSuccess();
			}

			@Override
			public Boolean get(long timeout, TimeUnit unit)
					throws InterruptedException, ExecutionException,
					TimeoutException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public boolean isCancelled() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public boolean isDone() {
				// TODO Auto-generated method stub
				return false;
			}
		};
	}
	
	public String get(String key) {
		try {
			return (String) asyncGet(key).get();
		} catch (InterruptedException e) {
			throw new RuntimeException("", e);
		} catch (ExecutionException e) {
			throw new RuntimeException("", e);
		}
	}
	
	public Future<Object> asyncGet(String key) {
		final CountDownLatch latch = new CountDownLatch(1);
		final Get command = new Get(latch, key.getBytes());
    	queue.add(command);
		session.write(command);
		return new Future<Object>() {
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public Object get() throws InterruptedException, ExecutionException {
				latch.await();
				return command.getValue();
			}

			@Override
			public Object get(long timeout, TimeUnit unit)
					throws InterruptedException, ExecutionException,
					TimeoutException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public boolean isCancelled() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public boolean isDone() {
				// TODO Auto-generated method stub
				return false;
			}
		};
	}
}
