package relay;

import pt.unl.fct.di.novasys.network.AttributeValidator;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.network.listeners.InConnListener;
import pt.unl.fct.di.novasys.network.listeners.MessageListener;
import pt.unl.fct.di.novasys.network.listeners.OutConnListener;
import relay.bandwidth.HostBandwidthList;
import relay.latency.SendMessageEvent;
import relay.messaging.RelayMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class BWLatencyRelay extends Relay implements InConnListener<RelayMessage>, OutConnListener<RelayMessage>, MessageListener<RelayMessage>, AttributeValidator {

	private final HostBandwidthList bwList;

	public BWLatencyRelay(Properties properties, InputStream hostsConfig, InputStream relayConfig, InputStream latencyConfig, InputStream bandwidthConfig) throws IOException {
		super(properties, hostsConfig, relayConfig, latencyConfig);

		bwList = new HostBandwidthList(peerList, bandwidthConfig);
	}

	protected void sendMessageWithDelay(RelayMessage msg) {
		Host receiver = msg.getTo();
		Host sender = msg.getFrom();

		if (peerToRelayConnections.containsKey(sender)) {
			float delay = calculateDelay(msg);
			Host relayHost = assignedRelayPerPeer.get(receiver);

			bwList.getOutBandwidthBucket(sender).enqueue(msg, () -> scheduler.addEvent(new SendMessageEvent(msg, () -> {
				if (relayHost.equals(self)) {
					if (!peerToRelayConnections.containsKey(receiver))
						throw new AssertionError("Null connection to peer " + receiver);
					bwList.getInBandwidthBucket(receiver).enqueue(msg, () -> sendMessage(msg, peerToRelayConnections.get(receiver)));
				} else {
					if (!otherRelayConnections.containsKey(relayHost))
						throw new AssertionError("Null connection to relay " + relayHost);
					sendMessage(msg, otherRelayConnections.get(relayHost));
				}
			}, delay)));
		} else {
			if (!peerToRelayConnections.containsKey(receiver))
				throw new AssertionError("Null connection to peer " + receiver);
			bwList.getInBandwidthBucket(receiver).enqueue(msg, () -> sendMessage(msg, peerToRelayConnections.get(receiver)));
		}
	}
}
