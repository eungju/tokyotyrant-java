package tokyotyrant.protocol;

import java.nio.ByteBuffer;


public abstract class CommandSupport<T> extends Command<T> {
	/**
	 * Requests are start with magic number.
	 */
	static PacketFormatBuilder magic() {
		return new PacketFormatBuilder().magic();
	}
	
	/**
	 * Responses are start with code.
	 */
	static PacketFormatBuilder code(boolean stopWhenError) {
		return new PacketFormatBuilder().code(stopWhenError);
	}

	/**
	 * Format of the request.
	 */
	private final PacketFormat requestFormat;
	
	/**
	 * Format of the response.
	 */
	private final PacketFormat responseFormat;
	
	public CommandSupport(byte commandId, PacketFormat requestFormat, PacketFormat responseFormat) {
		super(commandId);
		this.requestFormat = requestFormat;
		this.responseFormat = responseFormat;
	}

	public ByteBuffer encode() {
		PacketContext context = encodingContext();
		pack(context);
		return requestFormat.encode(context);
	}
	
	public boolean decode(ByteBuffer in) {
		PacketContext context = decodingContext();
		if (!responseFormat.decode(context, in)) {
			return false;
		}
		unpack(context);
		return true;
	}

	/**
	 * Create new context for encoding.
	 */
	PacketContext encodingContext() {
		PacketContext context = new PacketContext();
		context.put("magic", magic);
		return context;
	}

	/**
	 * Create new context for decoding.
	 */
	PacketContext decodingContext() {
		return new PacketContext();
	}

	/**
	 * Pack object properties into the packet context.
	 */
	protected abstract void pack(PacketContext context);

	/**
	 * Unpack object properties from the packet context.
	 */
	protected abstract void unpack(PacketContext context);
}
