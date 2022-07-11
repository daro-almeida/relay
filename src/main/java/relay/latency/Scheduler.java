package relay.latency;

import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

public class Scheduler {

	private final Map<Host, Queue<SendMessageEvent>> eventQueuePerPeer;
	private final Map<Host, Thread> eventThreadPerPeer;

	public Scheduler(List<Host> peerList) {
		eventQueuePerPeer = new HashMap<>(peerList.size());
		eventThreadPerPeer = new HashMap<>(peerList.size());
		for (Host peer : peerList) {
			Queue<SendMessageEvent> eventQueue = new PriorityBlockingQueue<>();
			eventQueuePerPeer.put(peer, eventQueue);
			Thread eventThread = new Thread(() -> this.eventLoop(eventQueue, new DefaultEventLoop()));
			eventThreadPerPeer.put(peer, eventThread);
			eventThread.start();
		}
	}

	private void eventLoop(Queue<SendMessageEvent> eventQueue, EventLoop loop) {
		while (true) {
			SendMessageEvent event = eventQueue.peek();

			long toSleep = event != null ? event.getSendTime() - System.currentTimeMillis() : Long.MAX_VALUE;

			if (toSleep <= 0) {
				SendMessageEvent e = eventQueue.remove();
				loop.submit(e.getRunnable());
			} else {
				try {
					Thread.sleep(toSleep);
				} catch (InterruptedException ignored) {
				}
			}
		}
	}

	public void addEvent(SendMessageEvent event) {
		Host sender = event.getMsg().getFrom();
		eventQueuePerPeer.get(sender).add(event);
		eventThreadPerPeer.get(sender).interrupt();
	}
}
