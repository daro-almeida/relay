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
		if (this.sendTime != other.sendTime)
			return (int) (this.sendTime - other.sendTime);
		else {
			if (this.msg.getSeqN() == -1 || other.msg.getSeqN() == -1 || !this.msg.getFrom().equals(other.msg.getFrom()))
				return 0;
			else
				return this.msg.getSeqN() - other.msg.getSeqN();
		}
	}
}
