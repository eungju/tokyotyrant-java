package tokyotyrant.command;

import java.nio.ByteBuffer;

import tokyotyrant.Command;

public abstract class CommandSupport<T> extends Command<T> {
	private final PacketFormat requestPacket;
	private final PacketFormat responsePacket;
	
	static PacketFormatBuilder packet() {
		return new PacketFormatBuilder();
	}
	
	public CommandSupport(byte commandId, PacketFormat requestPacket, PacketFormat responsePacket) {
		super(commandId);
		this.requestPacket = requestPacket;
		this.responsePacket = responsePacket;
	}

	public ByteBuffer encode() {
		PacketContext context = encodingContext();
		pack(context);
		return requestPacket.encode(context);
	}
	
	public boolean decode(ByteBuffer in) {
		PacketContext context = decodingContext();
		if (!responsePacket.decode(context, in)) {
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
