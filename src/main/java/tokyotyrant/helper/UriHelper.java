package tokyotyrant.helper;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UriHelper {
	public static URI[] getUris(String addresses) {
		List<URI> result = new ArrayList<URI>();
		for (String each : addresses.split("\\s")) {
			result.add(URI.create(each));
		}
		return result.toArray(new URI[result.size()]);
	}

	public static SocketAddress getSocketAddress(URI uri) {
		return new InetSocketAddress(uri.getHost(), uri.getPort());
	}
	
	public static Map<String, String> getParameters(URI uri) {
		Map<String, String>parameters = new HashMap<String, String>();
		if (uri.getQuery() != null) {
			String qs = uri.getQuery();
			for (String each : qs.split("&")) {
				String[] keyAndValue = each.split("=");
				parameters.put(keyAndValue[0], keyAndValue[1]);
			}
		}
		return parameters;
	}
}
