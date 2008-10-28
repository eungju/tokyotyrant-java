package org.zact.tokyotyrant;

import java.util.concurrent.BlockingQueue;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

public class TyrantEncoder implements ProtocolEncoder {
	public void dispose(IoSession session) throws Exception {
	}

	@SuppressWarnings("unchecked")
	public void encode(IoSession session, Object message, ProtocolEncoderOutput out) throws Exception {
		Command command = (Command)message;
		BlockingQueue<Command> queue = (BlockingQueue<Command>) session.getAttribute(TyrantProtocolHandler.COMMAND_QUEUE_KEY);
		queue.add(command);
		out.write(command.encode());
	}
}
