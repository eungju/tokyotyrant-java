package org.zact.tokyotyrant;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;

public class TyrantProtocolHandler extends IoHandlerAdapter {
	private LinkedBlockingQueue<Command> queue;
	
	public TyrantProtocolHandler(LinkedBlockingQueue<Command> queue) {
		this.queue = queue;
	}

    public void exceptionCaught(IoSession session, Throwable cause) {
        // Close connection when unexpected exception is caught.
        session.close();
    }

    public void messageReceived(IoSession session, Object message) throws Exception {
    	queue.add(((Command)message));
    }
}
