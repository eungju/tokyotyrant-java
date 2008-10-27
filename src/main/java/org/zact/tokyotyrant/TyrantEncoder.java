package org.zact.tokyotyrant;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

public class TyrantEncoder implements ProtocolEncoder {
	public void dispose(IoSession session) throws Exception {
	}

	public void encode(IoSession session, Object message, ProtocolEncoderOutput out) throws Exception {
		Command command = (Command)message;
		out.write(command.encode());
	}
}
