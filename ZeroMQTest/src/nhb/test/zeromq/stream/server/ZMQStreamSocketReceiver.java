package nhb.test.zeromq.stream.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.lmax.disruptor.EventHandler;
import com.nhb.common.Loggable;
import com.nhb.common.data.PuElement;

import nhb.test.zeromq.stream.MessagePieceCumulator;

final class ZMQStreamSocketReceiver implements EventHandler<MessagePieceEvent>, Loggable {

	private final Map<Integer, MessagePieceCumulator> connectedClients;

	ZMQStreamSocketReceiver() {
		this.connectedClients = new HashMap<>();
	}

	public static void main(String[] args) {
		System.out.println(Integer.MIN_VALUE);
	}

	@Override
	public void onEvent(MessagePieceEvent event, long sequence, boolean endOfBatch) throws Exception {

		int id = event.getId();
		MessagePieceCumulator cumulator = null;
		if (!connectedClients.containsKey(id)) {
			// new connection
			cumulator = new MessagePieceCumulator();
			connectedClients.put(id, cumulator);
			getLogger().debug("New connection: {}", id);
		} else {
			cumulator = connectedClients.get(id);
		}

		if (event.getData() != null && event.getData().length > 0) {
			if (cumulator != null) {
				List<PuElement> extractedMessages = cumulator.receive(event.getData());
				if (extractedMessages != null && !extractedMessages.isEmpty()) {
					event.setExtractedMessages(extractedMessages);
				}
			} else {
				getLogger().error("Something were wrong, the cumulator is null...");
			}
		} else {
			getLogger().debug("event's data null");
		}
	}
}