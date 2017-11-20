package nhb.test.zeromq.stream.client;

import com.nhb.common.data.PuElement;

public interface ZMQSocketMessageHandler {

	void onMessage(PuElement message);
}
