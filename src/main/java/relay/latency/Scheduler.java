package relay.latency;

import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

public class Scheduler {

	private final Thread eventThread;
	private final Queue<SendMessageEvent> eventQueue;
	private final Map<Host, EventLoop> loopPerSender;

	public Scheduler(List<Host> peerList) {
		eventQueue = new PriorityBlockingQueue<>();
		loopPerSender = new HashMap<Host, EventLoop>(peerList.size()) {{
			for (Host host : peerList) {
				put(host, new DefaultEventLoop());
			}
		}};
		eventThread = new Thread(this::eventLoop);
		eventThread.start();
	}

	private void eventLoop() {
		while (true) {
			SendMessageEvent event = eventQueue.peek();

			long toSleep = event != null ? event.getSendTime() - System.currentTimeMillis() : Long.MAX_VALUE;

			if (toSleep <= 0) {
				SendMessageEvent e = eventQueue.remove();
				loopPerSender.get(e.getMsg().getFrom()).submit(e.getRunnable());
			} else {
				try {
					Thread.sleep(toSleep);
				} catch (InterruptedException ignored) {
				}
			}
		}
	}

	public void addEvent(SendMessageEvent event) {
		eventQueue.add(event);
		eventThread.interrupt();
	}
}
