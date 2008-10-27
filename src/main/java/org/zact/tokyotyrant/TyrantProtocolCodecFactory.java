package org.zact.tokyotyrant;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;

public class TyrantProtocolCodecFactory implements ProtocolCodecFactory {
	public ProtocolDecoder getDecoder(IoSession session) throws Exception {
		return new TyrantDecoder();
	}

	public ProtocolEncoder getEncoder(IoSession session) throws Exception {
		return new TyrantEncoder();
	}
}
