package org.zact.tokyotyrant;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;

public class TyrantProtocolHandler extends IoHandlerAdapter {
    public void exceptionCaught(IoSession session, Throwable cause) {
        // Close connection when unexpected exception is caught.
        session.close();
    }

    public void messageSent(IoSession session, Object message) throws Exception {
    }

    public void messageReceived(IoSession session, Object message) throws Exception {
    }
}
