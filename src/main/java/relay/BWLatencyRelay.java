package relay;

import io.netty.channel.EventLoop;
import pt.unl.fct.di.novasys.network.AttributeValidator;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.network.listeners.InConnListener;
import pt.unl.fct.di.novasys.network.listeners.MessageListener;
import pt.unl.fct.di.novasys.network.listeners.OutConnListener;
import relay.bandwidth.HostBandwidthList;
import relay.messaging.RelayMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class BWLatencyRelay extends Relay implements InConnListener<RelayMessage>, OutConnListener<RelayMessage>, MessageListener<RelayMessage>, AttributeValidator {

	private final HostBandwidthList bwList;

	public BWLatencyRelay(Properties properties, InputStream hostsConfig, InputStream relayConfig, InputStream latencyConfig, InputStream bandwidthConfig) throws IOException {
		super(properties, hostsConfig, relayConfig, latencyConfig);

		bwList = new HostBandwidthList(peerList, bandwidthConfig);
	}

	protected void sendMessageWithDelay(RelayMessage msg) {
		Host receiver = msg.getTo();
		Host sender = msg.getFrom();

		EventLoop loop = loopPerSender.get(sender);
		if (loop == null) {
			assert peerToRelayConnections.get(receiver) != null;
			bwList.getInBandwidthBucket(receiver).enqueue(msg, () -> sendMessage(msg, peerToRelayConnections.get(receiver)));
		} else {
			long delay = calculateDelay(sender, receiver);

			bwList.getOutBandwidthBucket(sender).enqueue(msg, () -> loop.schedule(() -> {
				Host relayHost = assignedRelayPerPeer.get(receiver);
				if (relayHost.equals(self)) {
					assert peerToRelayConnections.get(receiver) != null;
					bwList.getInBandwidthBucket(receiver).enqueue(msg, () -> sendMessage(msg, peerToRelayConnections.get(receiver)));
				} else {
					assert otherRelayConnections.get(relayHost) != null;
					sendMessage(msg, otherRelayConnections.get(relayHost));
				}
			}, delay, TimeUnit.MICROSECONDS));
		}
	}
}
