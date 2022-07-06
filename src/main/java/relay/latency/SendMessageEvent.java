package relay.latency;

import relay.messaging.RelayMessage;

public class SendMessageEvent implements Comparable<SendMessageEvent> {

	private final RelayMessage msg;
	private final Runnable runnable;
	private final long sendTime;

	public SendMessageEvent(RelayMessage msg, Runnable runnable, float delay) {
		this.msg = msg;
		this.runnable = runnable;
		this.sendTime = System.currentTimeMillis() + Math.round(delay);
	}

	public Runnable getRunnable() {
		return runnable;
	}

	public long getSendTime() {
		return sendTime;
	}

	public RelayMessage getMsg() {
		return msg;
	}

	@Override
	public int compareTo(SendMessageEvent other) {
		if (this.sendTime == other.sendTime && this.msg.getSeqN() != -1 && other.msg.getSeqN() != -1)
			return this.msg.getSeqN() - other.msg.getSeqN();
		else
			return (int) (this.sendTime - other.sendTime);
	}
}
