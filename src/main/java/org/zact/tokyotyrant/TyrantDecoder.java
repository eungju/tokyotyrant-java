package org.zact.tokyotyrant;

import java.util.concurrent.BlockingQueue;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

public class TyrantDecoder extends CumulativeProtocolDecoder {
	@SuppressWarnings("unchecked")
	protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
		BlockingQueue<Command> queue = (BlockingQueue<Command>) session.getAttribute(TyrantProtocolHandler.COMMAND_QUEUE_KEY);
		Command command = queue.poll();
		if (command.decode(in)) {
			command.completed();
			out.write(command);
			return true;
		}
		return false;
	}
}
