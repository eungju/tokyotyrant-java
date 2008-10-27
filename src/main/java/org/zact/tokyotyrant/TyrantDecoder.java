package org.zact.tokyotyrant;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

public class TyrantDecoder extends CumulativeProtocolDecoder {
	private LinkedBlockingQueue<Command> queue;

	public TyrantDecoder(LinkedBlockingQueue<Command> queue) {
		super();
		this.queue = queue;
	}

	protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
		Command command = queue.poll();
		if (command.decode(in)) {
			command.completed();
			out.write(command);
			return true;
		}
		return false;
	}
}
