package nhb.test.zeromq.pubsub;

import java.nio.charset.Charset;
import java.util.StringTokenizer;

import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

public class Subscriber extends PubSubPatternTest {

	public static void main(String[] args) {
		new Subscriber().runTest();
	}

	private void startNewSubscriber(final String topic) {
		new Thread() {
			@Override
			public void run() {
				ZMQSocket subscriber = openSocket(PUB_SUB_ENDPOINT, ZMQSocketType.SUB);

				subscriber.subscribe(topic.getBytes());

				while (!Thread.currentThread().isInterrupted()) {
					// Process 100 updates
					int count;
					long total = 0;
					for (count = 0; count < 100; count++) {
						// Use trim to remove the tailing '0' character
						String string = subscriber.recvStr(Charset.defaultCharset()).trim();

						StringTokenizer sscanf = new StringTokenizer(string, " ");
						Integer.valueOf(sscanf.nextToken());
						int temperature = Integer.valueOf(sscanf.nextToken());
						Integer.valueOf(sscanf.nextToken());

						total += temperature;
					}
					System.out.println("Average temperature for zipcode '" + topic + "' was " + (int) (total / count));
				}
			}
		}.start();
	}

	@Override
	protected void test() throws Exception {
		startNewSubscriber("10001 ");
		startNewSubscriber("10002 ");
	}

}
