package tokyotyrant.command;

import java.util.HashMap;
import java.util.Map;

public class Stat extends CommandSupport<Map<String, String>> {
	private static final PacketFormat REQUEST = new PacketFormatBuilder().magic().end();
	private static final PacketFormat RESPONSE = new PacketFormatBuilder().code(false).int32("ssiz").bytes("sbuf", "ssiz").end();
	private Map<String, String> stat;
	             
	public Stat() {
		super((byte) 0x88, REQUEST, RESPONSE);
	}
	
	public Map<String, String> getReturnValue() {
		return stat;
	}
	
	protected void pack(PacketContext context) {
	}
	
	protected void unpack(PacketContext context) {
		code = (Byte)context.get("code");
		stat = parseTsv(new String((byte[])context.get("sbuf")));
	}
	
	Map<String, String> parseTsv(String tsv) {
		String[] lines = tsv.split("\\n");
		Map<String, String> pairs = new HashMap<String, String>();
		for (String line : lines) {
			String[] keyAndValue = line.split("\t");
			pairs.put(keyAndValue[0], keyAndValue[1]);
		}
		return pairs;
	}
}
