package tokyotyrant.networking;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class ReconnectQueue {
	static final int INITIAL_BACKOFF = 100;
	// maximum amount of time to wait between reconnect attempts
	static final int MAX_BACKOFF = 60 * 1000;
	
	private final SortedMap<Long, TokyoTyrantNode> queue = new TreeMap<Long, TokyoTyrantNode>();

	long now() {
		return System.currentTimeMillis();
	}
	
	int backoff(TokyoTyrantNode node) {
		return backoff(Math.min(node.getReconnectAttempt(), 16));
	}
	
	int backoff(int attempts) {
		return Math.min(INITIAL_BACKOFF * (1 << attempts), MAX_BACKOFF);
	}
	
	public void push(TokyoTyrantNode node) {
		assert !queue.containsValue(node);
		node.disconnect();
		node.reconnecting();
		queue.put(findEmptyTimeSlot(now() + backoff(node)), node);
	}
	
	long findEmptyTimeSlot(long timeSlot) {
		while (queue.containsKey(timeSlot)) {
			timeSlot++;
		}
		return timeSlot;
	}
	
	/**
	 * @return In millisecond.
	 */
	public long getTimeToNextAttempt() {
		if (queue.isEmpty()) {
			return 0;
		}
		return Math.max(queue.firstKey() - now(), 1);
	}
	
	public void reconnect() {
		List<TokyoTyrantNode> failedNodes = new ArrayList<TokyoTyrantNode>();
		for (Iterator<TokyoTyrantNode> i = queue.headMap(now()).values().iterator(); i.hasNext(); ) {
			TokyoTyrantNode node = i.next();
			i.remove();
			if (!node.connect()) {
				failedNodes.add(node);
			}
		}
		for (TokyoTyrantNode each : failedNodes) {
			push(each);
		}
	}
	
	public int countDelayed() {
		return queue.size();
	}
}
