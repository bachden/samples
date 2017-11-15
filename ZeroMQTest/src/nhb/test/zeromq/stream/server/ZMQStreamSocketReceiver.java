package nhb.test.zeromq.stream.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.lmax.disruptor.EventHandler;
import com.nhb.common.Loggable;
import com.nhb.common.data.PuElement;
import com.nhb.common.vo.ByteArrayWrapper;

final class ZMQStreamSocketReceiver implements EventHandler<MessagePieceEvent>, Loggable {

	private final Map<ByteArrayWrapper, MessagePieceCumulator> connectedClients;

	ZMQStreamSocketReceiver() {
		this.connectedClients = new HashMap<>();
	}

	@Override
	public void onEvent(MessagePieceEvent event, long sequence, boolean endOfBatch) throws Exception {

		ByteArrayWrapper id = event.getId();
		MessagePieceCumulator messagePieceCumulator = connectedClients.get(id);

		if (event.getData() == null || event.getData().length == 0) {
			if (!connectedClients.containsKey(id)) {
				// new connection
				connectedClients.put(id, new MessagePieceCumulator());
				getLogger().debug("New connection: {} -> hash: {}", id, id.hashCode());
			} else {
				// connection closed
				connectedClients.remove(id);
				getLogger().debug("Client id {} is disconnected, total received messages: {}", id,
						messagePieceCumulator.getTotalReceivedMessages());
			}
		} else {
			// System.out.println("Received from " + id + ": " + new
			// String(event.getData()));
			if (messagePieceCumulator != null) {
				List<PuElement> extractedMessages = messagePieceCumulator.receive(event.getData());
				if (extractedMessages != null && !extractedMessages.isEmpty()) {
					event.setExtractedMessages(extractedMessages);
				}
			} else {
				getLogger().error("Something were wrong, the cumulator is null...");
			}
		}
	}
}