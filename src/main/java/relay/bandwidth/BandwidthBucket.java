package relay.bandwidth;

import relay.bandwidth.units.BitUnit;
import relay.bandwidth.units.ByteUnit;

import java.util.Timer;
import java.util.TimerTask;

public class BandwidthBucket {

	private static final short CONTROL_PACKET_SIZE = 20;
	private static final int FREQUENCY = 1000;
	private static final ByteUnit BUCKET_UNIT = ByteUnit.BYTE;
	private double capacity;
	private double currentSize;
	private final Timer timer;

	private BandwidthBucket() {
		this.currentSize = 0;
		this.timer = new Timer();
	}

	public BandwidthBucket(double capacity, ByteUnit unit) {
		this();
		this.capacity = BUCKET_UNIT.convert(capacity, unit);
	}

	public BandwidthBucket(double capacity, BitUnit unit) {
		this();
		this.capacity = BUCKET_UNIT.convert(capacity, unit);
	}

	public void addNormalPacketAndWait(int contentSize) {
		addToBucket(CONTROL_PACKET_SIZE + contentSize);
	}

	public void addControlPacketAndWait() {
		addToBucket(CONTROL_PACKET_SIZE);
	}

	private void addToBucket(double amount) {
		if (amount <= 0)
			return;
		if (isEmpty())
			startTimer();
		currentSize += amount; //BUCKET_UNIT.convert(amount, ByteUnit.BYTE);

		synchronized(this) {
			while(isFull()) {
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
					currentSize = Math.max(currentSize - capacity /* * (FREQUENCY/1000)*/, 0);
					if (isEmpty())
						timer.cancel();
					if (!isFull())
						this.notify();
				}
			}
		}, FREQUENCY, FREQUENCY);
	}

	private boolean isFull() {
		return currentSize >= capacity;
	}

	private boolean isEmpty() {
		return currentSize == 0;
	}
}
