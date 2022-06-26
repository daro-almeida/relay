package relay.bandwidth;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import relay.bandwidth.units.BitUnit;
import relay.bandwidth.units.ByteUnit;
import relay.messaging.RelayAppMessage;
import relay.messaging.RelayMessage;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

public class BandwidthBucket {

	private static final Logger logger = LogManager.getLogger(BandwidthBucket.class);

	private static final short CONTROL_PACKET_SIZE = 20;
	private static final int FREQUENCY = 1;
	private static final ByteUnit BUCKET_UNIT = ByteUnit.BYTE;

	private final Timer timer;
	private final Queue<MutablePair<Double, Runnable>> queue;
	private double capacity;
	private double currentSize;

	private BandwidthBucket() {
		this.currentSize = 0;
		this.queue = new LinkedList<>();
		this.timer = new Timer();
		startTimer();
	}

	public BandwidthBucket(double capacity, ByteUnit unit) {
		this();
		this.capacity = BUCKET_UNIT.convert(capacity, unit);
	}

	public BandwidthBucket(double capacity, BitUnit unit) {
		this();
		this.capacity = BUCKET_UNIT.convert(capacity, unit);
	}

	private void addNormalPacket(int contentSize, Runnable runnable) {
		addToBucket(CONTROL_PACKET_SIZE + contentSize, runnable);
	}

	private void addControlPacket(Runnable runnable) {
		addToBucket(CONTROL_PACKET_SIZE, runnable);
	}

	private void addToBucket(double amount, Runnable runnable) {
		if (amount <= 0)
			return;

		if (isFull()) {
			queue.add(new MutablePair<>(amount, runnable));
			logger.trace("Queue done. {}/{}, {}", currentSize, capacity, queue.size());
		} else {
			currentSize += amount;
			if(isFull()) {
				queue.add(new MutablePair<>(0D, runnable));
				logger.trace("Queue done. {}/{}, {}", currentSize, capacity, queue.size());
			} else
				new Thread(runnable).start();
		}

	}

	private void startTimer() {
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				currentSize = Math.max(currentSize - capacity * ((float) FREQUENCY/1000), 0);
				while (!isFull() && !queue.isEmpty()) {
					MutablePair<Double, Runnable> p = queue.peek();
					if (p != null) {
						currentSize += p.getLeft();
						if (isFull())
							p.setLeft(0D);
						else {
							queue.remove();
							logger.trace("Dequeue done. {}/{}, {}", currentSize, capacity, queue.size());
							new Thread(p.getRight()).start();
						}
					}
				}
			}
		}, FREQUENCY, FREQUENCY);
	}

	private boolean isFull() {
		return currentSize > capacity;
	}

	public void enqueue(RelayMessage msg, Runnable runnable) {
		switch (msg.getType()) {
			case APP_MSG:
				addNormalPacket(((RelayAppMessage) msg).getPayload().length, runnable);
				break;
			case CONN_OPEN:
			case CONN_CLOSE:
			case CONN_ACCEPT:
			case CONN_FAIL:
			case PEER_DEAD:
				addControlPacket(runnable);
		}
	}
}
