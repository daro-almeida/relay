package relay;

import io.netty.channel.EventLoopGroup;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.AttributeValidator;
import pt.unl.fct.di.novasys.network.Connection;
import pt.unl.fct.di.novasys.network.NetworkManager;
import pt.unl.fct.di.novasys.network.data.Attributes;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.network.listeners.InConnListener;
import pt.unl.fct.di.novasys.network.listeners.MessageListener;
import pt.unl.fct.di.novasys.network.listeners.OutConnListener;
import relay.latency.LatencyMatrix;
import relay.latency.Scheduler;
import relay.latency.SendMessageEvent;
import relay.messaging.*;
import relay.util.ConfigUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Relay implements InConnListener<RelayMessage>, OutConnListener<RelayMessage>, MessageListener<RelayMessage>, AttributeValidator {

	public static final String NAME = "Relay";
	public static final String ADDRESS_KEY = "address";
	public static final String PORT_KEY = "port";
	public static final String HEARTBEAT_INTERVAL_KEY = "heartbeat_interval";
	public static final String HEARTBEAT_TOLERANCE_KEY = "heartbeat_tolerance";
	public static final String CONNECT_TIMEOUT_KEY = "connect_timeout";
	public static final String RELAY_ID = "relay_id";
	public static final String NUM_RELAYS = "num_relays";
	public static final String NUM_NODES = "num_peers";
	public static final String SLEEP = "sleep";
	public static final String WORKER_GROUP_KEY = "workerGroup";
	public static final String LISTEN_ADDRESS_ATTRIBUTE = "listen_address";
	public static final String DEFAULT_PORT = "9082";
	public static final String DEFAULT_HB_INTERVAL = "0";
	public static final String DEFAULT_HB_TOLERANCE = "0";
	public static final String DEFAULT_CONNECT_TIMEOUT = "1000";

	private static final float DEFAULT_LATENCY = 0;
	private static final Logger logger = LogManager.getLogger(Relay.class);
	private static final Short EMULATED_MAGIC_NUMBER = 0x1369;

	protected final Map<Host, Connection<RelayMessage>> peerToRelayConnections;
	protected final Map<Host, Connection<RelayMessage>> otherRelayConnections;
	protected final Map<Host, Host> assignedRelayPerPeer;
	protected final Host self;
	protected final List<Host> peerList;
	protected final Scheduler scheduler;

	private final NetworkManager<RelayMessage> network;
	private final Attributes attributes;
	// left -> right
	private final Set<Pair<Host, Host>> peerToPeerConnections;
	private final Set<Host> disconnectedPeers;
	private final LatencyMatrix latencyMatrix;
	private final int relayID;
	private final List<Host> relayList;
	private final Set<Host> relaySet;


	public Relay(Properties properties, InputStream hostsConfig, InputStream relayConfig, InputStream latencyConfig) throws IOException {
		InetAddress address;
		if (properties.containsKey(ADDRESS_KEY)) address = InetAddress.getByName(properties.getProperty(ADDRESS_KEY));
		else throw new IllegalArgumentException(NAME + " requires binding address");

		int port = Integer.parseInt(properties.getProperty(PORT_KEY, DEFAULT_PORT));
		int numPeers = Integer.parseInt(properties.getProperty(NUM_NODES, "0"));
		int numRelays = Integer.parseInt(properties.getProperty(NUM_RELAYS, "1"));
		relayID = Integer.parseInt(properties.getProperty(RELAY_ID, "0"));

		int hbInterval = Integer.parseInt(properties.getProperty(HEARTBEAT_INTERVAL_KEY, DEFAULT_HB_INTERVAL));
		int hbTolerance = Integer.parseInt(properties.getProperty(HEARTBEAT_TOLERANCE_KEY, DEFAULT_HB_TOLERANCE));
		int connTimeout = Integer.parseInt(properties.getProperty(CONNECT_TIMEOUT_KEY, DEFAULT_CONNECT_TIMEOUT));

		self = new Host(address, port);

		int numPeersOfThisRelay = numPeersOfRelay(numPeers, relayID, numRelays);

		EventLoopGroup eventExecutors = properties.containsKey(WORKER_GROUP_KEY) ? (EventLoopGroup) properties.get(WORKER_GROUP_KEY) : NetworkManager.createNewWorkerGroup();
		RelayMessageSerializer tRelayMessageSerializer = new RelayMessageSerializer();
		network = new NetworkManager<>(tRelayMessageSerializer, this, hbInterval, hbTolerance, connTimeout);
		network.createServerSocket(this, self, this, eventExecutors);

		attributes = new Attributes();
		attributes.putShort(AttributeValidator.CHANNEL_MAGIC_ATTRIBUTE, EMULATED_MAGIC_NUMBER);
		attributes.putHost(LISTEN_ADDRESS_ATTRIBUTE, self);

		peerToRelayConnections = new ConcurrentHashMap<>(numPeersOfThisRelay);

		peerToPeerConnections = ConcurrentHashMap.newKeySet(numPeersOfThisRelay * (numPeers - 1));

		disconnectedPeers = ConcurrentHashMap.newKeySet(numPeersOfThisRelay);

		otherRelayConnections = new ConcurrentHashMap<>(numRelays - 1);
		assignedRelayPerPeer = new HashMap<>(numPeers);

		peerList = ConfigUtils.configToHostList(hostsConfig, numPeers);
		Pair<Integer, Integer> range = peerRange(numPeers, relayID, numRelays);
		latencyMatrix = new LatencyMatrix(peerList, latencyConfig, range.getLeft(), range.getRight());

		relayList = ConfigUtils.configToHostList(relayConfig, numRelays);
		relaySet = new HashSet<>(relayList);

		scheduler = new Scheduler(peerList);

		assignPeersToRelays(numRelays, numPeers);

		if (numRelays > 1) {
			int sleep = Integer.parseInt(properties.getProperty(SLEEP));
			new Timer().schedule(new TimerTask() {
				@Override
				public void run() {
					connectToRelays(numRelays);
				}
			}, sleep);
		}
	}

	private static int numPeersOfRelay(int numPeers, int relayID, int numRelays) {
		int r = numPeers % numRelays;
		return numPeers / numRelays + ((r > relayID) ? 1 : 0);
	}

	private static Pair<Integer, Integer> peerRange(int numPeers, int relayID, int numRelays) {
		int r = numPeers % numRelays;
		int size = numPeers / numRelays + ((r > relayID) ? 1 : 0);
		int start = relayID * (numPeers / numRelays) + Math.max(0, Math.min(r, relayID));
		int end = start + size - 1;

		return new ImmutablePair<>(start, end);
	}

	private void connectToRelays(int numRelays) {
		for (int i = relayID + 1; i < numRelays; i++) {
			network.createConnection(relayList.get(i), attributes, this);
		}
	}

	private void assignPeersToRelays(int numRelays, int numPeers) {
		for (int i = 0; i < numRelays; i++) {
			Pair<Integer, Integer> range = peerRange(numPeers, i, numRelays);
			for (int j = range.getLeft(); j <= range.getRight(); j++) {
				assignedRelayPerPeer.put(peerList.get(j), relayList.get(i));
			}
		}
	}

	public void disconnectPeer(Host peer) {
		logger.debug("Disconnecting peer: {}", peer);

		if (!peerToRelayConnections.containsKey(peer)) {
			logger.debug("Disconnecting peer: Peer to disconnect not connected to relay.");
			return;
		}

		if (!disconnectedPeers.add(peer)) {
			logger.debug("Disconnecting peer: Peer already disconnected.");
			return;
		}

		//send disconnect notification instantly to peer getting disconnected, it is assumed it is connected to this relay
		sendMessage(new RelayPeerDisconnectedMessage(peer, peer, new IOException("Node " + peer + " disconnected")), peerToRelayConnections.get(peer));

		sendPeerDisconnectNotifications(peer);
	}

	private void sendPeerDisconnectNotifications(Host peer) {
		Iterator<Pair<Host, Host>> it = peerToPeerConnections.iterator();

		while (it.hasNext()) {
			Pair<Host, Host> pair = it.next();
			Host other;
			if (pair.getLeft().equals(peer)) {
				other = pair.getRight();
			} else if (pair.getRight().equals(peer)) {
				other = pair.getLeft();
			} else continue;

			sendMessageWithDelay(new RelayPeerDisconnectedMessage(peer, other, new IOException("Node " + peer + " disconnected")));

			it.remove();
		}
	}

	public void reconnectPeer(Host peer) {
		logger.debug("Reconnecting peer: {}", peer);

		if (!peerToRelayConnections.containsKey(peer)) {
			logger.debug("Reconnecting peer: Peer to connect not connected to relay.");
			return;
		}

		if (!disconnectedPeers.remove(peer)) logger.debug("Reconnecting peer: Peer already connected.");
		else
			//send signal that peer is reconnected to network
			sendMessage(new RelayPeerDisconnectedMessage(peer, peer, new IOException("Node " + peer + " reconnected")), peerToRelayConnections.get(peer));
	}

	@Override
	public void inboundConnectionUp(Connection<RelayMessage> connection) {
		Host clientSocket = null;
		try {
			clientSocket = connection.getPeerAttributes().getHost(LISTEN_ADDRESS_ATTRIBUTE);
		} catch (IOException ex) {
			logger.fatal("Error parsing LISTEN_ADDRESS_ATTRIBUTE of inbound connection: " + ex.getMessage());
		}

		if (clientSocket == null) {
			logger.fatal("Inbound connection without LISTEN_ADDRESS: " + connection.getPeer() + " " + connection.getPeerAttributes());
		} else {

			logger.debug("InboundConnectionUp {}", clientSocket);
			Connection<RelayMessage> old;
			if (relaySet.contains(clientSocket)) {
				logger.info("Relay {} connected", clientSocket);
				old = otherRelayConnections.put(clientSocket, connection);
			} else {
				logger.info("Peer {} connected", clientSocket);
				old = peerToRelayConnections.put(clientSocket, connection);
			}

			if (old != null)
				logger.fatal("Double incoming connection from: " + clientSocket + " (" + connection.getPeer() + ")");
		}
	}

	@Override
	public void inboundConnectionDown(Connection<RelayMessage> connection, Throwable cause) {
		Host clientSocket = null;
		try {
			clientSocket = connection.getPeerAttributes().getHost(LISTEN_ADDRESS_ATTRIBUTE);
		} catch (IOException ex) {
			logger.fatal("Inbound connection without valid listen address in connectionDown: " + ex.getMessage());
		}

		if (clientSocket == null) {
			logger.fatal("Inbound connection without LISTEN_ADDRESS: " + connection.getPeer() + " " + connection.getPeerAttributes());
		} else {
			if (relaySet.contains(clientSocket))
				logger.fatal("Relay " + clientSocket + " disconnected from relay unexpectedly! cause:" + ((cause == null) ? "" : " " + Arrays.toString(cause.getStackTrace())));
			else
				logger.fatal("Peer " + clientSocket + " disconnected from relay unexpectedly! cause:" + ((cause == null) ? "" : " " + Arrays.toString(cause.getStackTrace())));
		}
	}

	@Override
	public void outboundConnectionUp(Connection<RelayMessage> connection) {
		Host clientSocket = connection.getPeer();

		logger.debug("OutboundConnectionUp {}", clientSocket);
		Connection<RelayMessage> old = this.otherRelayConnections.put(clientSocket, connection);

		if (old != null)
			logger.fatal("Double outgoing connection with: " + clientSocket + " (" + connection.getPeer() + ")");
	}

	@Override
	public void outboundConnectionDown(Connection<RelayMessage> connection, Throwable cause) {
		Host clientSocket = null;
		try {
			clientSocket = connection.getPeerAttributes().getHost(LISTEN_ADDRESS_ATTRIBUTE);
		} catch (IOException ex) {
			logger.fatal("Outbound connection without valid listen address in connectionDown: " + ex.getMessage());
		}

		if (clientSocket == null) {
			logger.fatal("Outbound connection without LISTEN_ADDRESS: " + connection.getPeer() + " " + connection.getPeerAttributes());
		} else {
			logger.fatal("Relay " + clientSocket + " disconnected unexpectedly! cause:" + ((cause == null) ? "" : " " + Arrays.toString(cause.getStackTrace())));
		}
	}

	@Override
	public void outboundConnectionFailed(Connection<RelayMessage> connection, Throwable cause) {
		outboundConnectionDown(connection, cause);
	}

	public void serverSocketBind(boolean success, Throwable cause) {
		if (success) {
			logger.trace("Server socket ready");
		} else {
			logger.fatal("Server socket bind failed: " + cause);
		}

	}

	public void serverSocketClose(boolean success, Throwable cause) {
		logger.fatal("Server socket closed. " + (success ? "" : "Cause: " + cause));
	}

	@Override
	public void deliverMessage(RelayMessage msg, Connection<RelayMessage> connection) {
		Host from = msg.getFrom();
		Host to = msg.getTo();

		RelayMessage.Type type = msg.getType();

		logger.trace("Received {} message {} to {} from {}", type.name(), msg.getSeqN(), to, from);

		if (disconnectedPeers.contains(to)) {
			if (type == RelayMessage.Type.CONN_OPEN)
				sendMessageWithDelay(new RelayConnectionFailMessage(to, from, new IOException("Peer " + to + " is disconnected.")));
			return;
		}

		switch (type) {
			case APP_MSG:
				handleAppMessage((RelayAppMessage) msg);
				break;
			case CONN_OPEN:
				handleConnectionRequest((RelayConnectionOpenMessage) msg);
				break;
			case CONN_CLOSE:
				handleConnectionClose((RelayConnectionCloseMessage) msg);
				break;
			case CONN_ACCEPT:
				handleConnectionAccept((RelayConnectionAcceptMessage) msg);
				break;
			case CONN_FAIL:
			case PEER_DISCONNECTED:
				sendMessageWithDelay(msg);
		}
	}

	private void handleConnectionClose(RelayConnectionCloseMessage msg) {
		Host to = msg.getTo();
		Host from = msg.getFrom();

		logger.debug("Connection close message to {} from {}", to, from);

		if (!peerToPeerConnections.remove(new ImmutablePair<>(from, to))) {
			logger.debug("Connection close with no out connection from {} to {}", from, to);
			return;
		}

		sendMessageWithDelay(msg);
	}

	private void handleConnectionAccept(RelayConnectionAcceptMessage msg) {
		Host to = msg.getTo();
		Host from = msg.getFrom();

		logger.debug("Connection accepted message to {} from {}", to, from);

		if (!peerToPeerConnections.contains(new ImmutablePair<>(to, from))) {
			logger.debug("Connection accept with no out connection from {} to {}", to, from);
			return;
		}

		sendMessageWithDelay(msg);
	}

	private void handleAppMessage(RelayAppMessage msg) {
		Host to = msg.getTo();
		Host from = msg.getFrom();

		logger.debug("Message to {} from {}", to, from);

		if (!peerToPeerConnections.contains(new ImmutablePair<>(from, to)) && !peerToPeerConnections.contains(new ImmutablePair<>(to, from))) {
			logger.debug("No connection between {} and {}", from, to);
			return;
		}

		sendMessageWithDelay(msg);
	}

	private void handleConnectionRequest(RelayConnectionOpenMessage msg) {
		Host to = msg.getTo();
		Host from = msg.getFrom();

		logger.debug("Connection request to {} from {}", to, from);

		Pair<Host, Host> pair = new ImmutablePair<>(from, to);
		if (peerToPeerConnections.contains(new ImmutablePair<>(from, to))) {
			logger.debug("Connection request when already existing connection: {}-{}", from, to);
			return;
		}

		sendMessageWithDelay(msg);
		peerToPeerConnections.add(pair);
	}

	@Override
	public boolean validateAttributes(Attributes attr) {
		Short channel = attr.getShort(CHANNEL_MAGIC_ATTRIBUTE);
		return channel != null && channel.equals(EMULATED_MAGIC_NUMBER);
	}

	protected void sendMessageWithDelay(RelayMessage msg) {
		Host receiver = msg.getTo();

		Connection<RelayMessage> con;
		if ((con = peerToRelayConnections.get(receiver)) != null) {
			float delay = calculateDelay(msg);
			scheduler.addEvent(new SendMessageEvent(msg, () -> sendMessage(msg, con), delay));
		} else {
			sendMessage(msg, otherRelayConnections.get(assignedRelayPerPeer.get(receiver)));
		}
	}

	public float calculateDelay(RelayMessage msg) {
		Host sender = msg.getFrom();
		Host receiver = msg.getTo();

		Float latency = latencyMatrix.getProperty(sender, receiver);
		if (latency == null) {
			logger.error("Null latency: {}-{}", sender, receiver);
			latency = DEFAULT_LATENCY;
		}

		return latency - (System.currentTimeMillis() - msg.getSentTime());
	}

	protected void sendMessage(RelayMessage msg, Connection<RelayMessage> con) {
		if (!disconnectedPeers.contains(msg.getTo())) {
			if (con == null) {
				logger.error("Null connection with msg {}", msg);
			} else {
				con.sendMessage(msg);
				logger.trace("Sending {} message {} to {} from {}", msg.getType().name(), msg.getSeqN(), msg.getTo(), msg.getFrom());
			}
		}
	}

}
