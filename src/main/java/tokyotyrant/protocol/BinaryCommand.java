package tokyotyrant.protocol;

import tokyotyrant.transcoder.Transcoder;

public abstract class BinaryCommand<T> extends Command<T> {
    public static final byte ESUCCESS = 0x00;
    public static final byte EUNKNOWN = (byte) 0xff;

    protected final Transcoder keyTranscoder;
    protected final Transcoder valueTranscoder;
    protected final byte[] magic;
    protected byte code;

    public BinaryCommand(byte commandId, Transcoder keyTranscoder, Transcoder valueTranscoder) {
        magic = new byte[] {(byte) 0xC8, commandId};
        code = EUNKNOWN;
        this.keyTranscoder = keyTranscoder;
        this.valueTranscoder = valueTranscoder;
    }

    @Override
    public boolean responseRequired() {
        return true;
    }

    protected boolean isSuccess() {
        return code == ESUCCESS;
    }
}
