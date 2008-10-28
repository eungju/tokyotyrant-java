package org.zact.tokyotyrant;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.AttributeKey;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TyrantProtocolHandler extends IoHandlerAdapter {
	public static final AttributeKey COMMAND_QUEUE_KEY = new AttributeKey(TyrantProtocolHandler.class, "COMMAND_QUEUE");
    private final Logger logger = LoggerFactory.getLogger(getClass());

	public void exceptionCaught(IoSession session, Throwable cause) {
        // Close connection when unexpected exception is caught.
        session.close();
    }

    public void sessionOpened(IoSession session) throws Exception {
    	BlockingQueue<Command> queue = new LinkedBlockingQueue<Command>();
    	session.setAttribute(COMMAND_QUEUE_KEY, queue);
    	logger.debug("Opened");
    }

    public void sessionClosed(IoSession session) throws Exception {
    	session.removeAttribute(COMMAND_QUEUE_KEY);
    	logger.debug("Closed");
    }

    public void messageSent(IoSession session, Object message) throws Exception {
    	logger.debug("Sent " + message);
    }

    public void messageReceived(IoSession session, Object message) throws Exception {
    	logger.debug("Received " + message);
    }
}
