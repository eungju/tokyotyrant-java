package tokyotyrant;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
	
	@Test public void whenCancel() {
		DummyCommand command = new DummyCommand();
		Future<Object> future = new CommandFuture<Object>(command);
		future.cancel(true);
		assertTrue(command.isCancelled());
		assertTrue(future.isDone());
	}
}
