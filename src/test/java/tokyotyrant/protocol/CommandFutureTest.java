package tokyotyrant.protocol;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

public class CommandFutureTest {
	static class DummyCommand extends Command<Object> {
		public DummyCommand() {
			super((byte)0xff);
		}
		
		public boolean decode(ByteBuffer in) {
			return true;
		}

		public ByteBuffer encode() {
			return null;
		}

		public Object getReturnValue() {
			return 42;
		}
	};
	
	@Test public void whenComplete() throws InterruptedException, ExecutionException {
		DummyCommand command = new DummyCommand();
		Future<Object> future = new CommandFuture<Object>(command);
		assertFalse(command.isReading());
		command.reading();
		assertTrue(command.isReading());
		assertFalse(future.isDone());
		command.complete();
		assertTrue(future.isDone());
		assertEquals(42, future.get());
	}

	@Test(expected=ExecutionException.class) public void whenError() throws InterruptedException, ExecutionException {
		DummyCommand command = new DummyCommand();
		Future<Object> future = new CommandFuture<Object>(command);
		Exception exception = new Exception();
		command.error(exception);
		assertTrue(command.hasError());
		assertSame(exception, command.getErrorException());
		assertTrue(future.isDone());
		future.get();
	}
	
	@Test(expected=ExecutionException.class)
	public void whenCancel() throws InterruptedException, ExecutionException {
		DummyCommand command = new DummyCommand();
		Future<Object> future = new CommandFuture<Object>(command);
		future.cancel(true);
		assertTrue(command.isCancelled());
		assertTrue(future.isDone());
		future.get();
	}
	
	@Test(expected=TimeoutException.class)
	public void getThrowsTimeoutExceptionWhenTimeoutExpired() throws InterruptedException, ExecutionException, TimeoutException {
		DummyCommand command = new DummyCommand();
		Future<Object> future = new CommandFuture<Object>(command);
		future.get(10, TimeUnit.MILLISECONDS);
	}
}
