package org.zact.tokyotyrant;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

public class TyrantDecoder extends CumulativeProtocolDecoder {
	protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
		Command command = (Command) session.getAttribute(TyrantConnection.COMMAND_KEY);
		if (command.decode(in)) {
			out.write(command);
			return true;
		}
		return false;
	}
}
