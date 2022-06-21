package relay.bandwidth;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import relay.bandwidth.units.BitUnit;
import relay.bandwidth.units.ByteUnit;
import relay.messaging.RelayAppMessage;
import relay.messaging.RelayMessage;

import java.util.Timer;
import java.util.TimerTask;

public class BandwidthBucket {

	private static final Logger logger = LogManager.getLogger(BandwidthBucket.class);

	private static final short CONTROL_PACKET_SIZE = 20;
	private static final int FREQUENCY = 1;
	private static final ByteUnit BUCKET_UNIT = ByteUnit.BYTE;
	private final Timer timer;
	private double capacity;
	private double currentSize;

	private BandwidthBucket() {
		this.currentSize = 0;
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

	private void addNormalPacketAndWait(int contentSize) {
		addToBucket(CONTROL_PACKET_SIZE + contentSize);
	}

	private void addControlPacketAndWait() {
		addToBucket(CONTROL_PACKET_SIZE);
	}

	private void addToBucket(double amount) {
		if (amount <= 0)
			return;
		currentSize += amount;
		if(currentSize > capacity) {
			logger.trace(currentSize + "/" + capacity);
		}

		synchronized (this) {
			while (isFull()) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					//do nothing
				}
			}
		}
	}

	private void startTimer() {
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				synchronized (this) {
					currentSize = Math.max(currentSize - capacity * ((float) FREQUENCY/1000), 0);
					if (!isFull())
						this.notify();
				}
			}
		}, FREQUENCY, FREQUENCY);
	}

	private boolean isFull() {
		return currentSize > capacity;
	}

	public void addPacketAndWait(RelayMessage msg) {
		switch (msg.getType()) {
			case APP_MSG:
				addNormalPacketAndWait(((RelayAppMessage) msg).getPayload().length);
				break;
			case CONN_OPEN:
			case CONN_CLOSE:
			case CONN_ACCEPT:
			case CONN_FAIL:
			case PEER_DEAD:
				addControlPacketAndWait();
		}
	}
}
