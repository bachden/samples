package nhb.test.zeromq.pubsub;

import java.util.Random;

import com.nhb.messaging.zmq.ZMQSocket;
import com.nhb.messaging.zmq.ZMQSocketType;

public class Publisher extends PubSubPatternTest {

	public static void main(String[] args) {
		new Publisher().runTest();
	}

	@Override
	protected void test() throws Exception {
		ZMQSocket publisher = this.openSocket(PUB_SUB_ENDPOINT, ZMQSocketType.PUB);

		Thread.sleep(1000);

		// Initialize random number generator
		Random srandom = new Random(System.currentTimeMillis());
		while (!Thread.currentThread().isInterrupted()) {
			// Get values that will fool the boss
			int zipcode, temperature, relhumidity;
			zipcode = 10000 + srandom.nextInt(10);
			temperature = srandom.nextInt(215) - 80 + 1;
			relhumidity = srandom.nextInt(50) + 10 + 1;

			// Send message to all subscribers
			String update = String.format("%05d %d %d", zipcode, temperature, relhumidity);
			publisher.send(update, 0);
		}
	}

}
