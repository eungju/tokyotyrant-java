package org.zact.tokyotyrant;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;

public class TyrantProtocolCodecFactory implements ProtocolCodecFactory {
	private TyrantEncoder encoder;
	private TyrantDecoder decoder;

	public TyrantProtocolCodecFactory(LinkedBlockingQueue<Command> queue) {
		this.encoder = new TyrantEncoder();
		this.decoder = new TyrantDecoder(queue);
	}

	public ProtocolDecoder getDecoder(IoSession session) throws Exception {
		return decoder;
	}

	public ProtocolEncoder getEncoder(IoSession session) throws Exception {
		return encoder;
	}
}
