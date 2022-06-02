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
import relay.messaging.*;
import relay.util.Utils;
import relay.util.matrixes.ShortMatrix;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Relay implements InConnListener<RelayMessage>, OutConnListener<RelayMessage>, MessageListener<RelayMessage>, AttributeValidator {

	public static final String NAME = "Relay";
	public static final String ADDRESS_KEY = "address";
	public static final String PORT_KEY = "port";
	public static final String HEARTBEAT_INTERVAL_KEY = "heartbeat_interval";
	public static final String HEARTBEAT_TOLERANCE_KEY = "heartbeat_tolerance";
	public static final String CONNECT_TIMEOUT_KEY = "connect_timeout";
	public static final String RELAY_ID = "relay_id";
	public static final String NUM_RELAYS = "num_relays";
	public static final String NUM_PROCESSES = "num_processes";
	public static final String WORKER_GROUP_KEY = "workerGroup";
	public static final String LISTEN_ADDRESS_ATTRIBUTE = "listen_address";
	public static final String DEFAULT_PORT = "9082";
	public static final String DEFAULT_HB_INTERVAL = "0";
	public static final String DEFAULT_HB_TOLERANCE = "0";
	public static final String DEFAULT_CONNECT_TIMEOUT = "1000";
	private static final Short DEFAULT_DELAY = 0;
	private static final Short AVERAGE_ERROR = 2;
	private static final long CONNECT_RELAYS_WAIT = 5000;
	private static final Logger logger = LogManager.getLogger(Relay.class);
	private static final Short PROXY_MAGIC_NUMBER = 0x1369;

	static {
		System.setProperty("log4j.configurationFile", "log4j2.xml");
	}

	private final NetworkManager<RelayMessage> network;
	private final Attributes attributes;

	// left -> right
	private final Set<Pair<Host, Host>> peerToPeerConnections;
	private final Set<Host> disconnectedPeers;

	private final ShortMatrix latencyMatrix;

	private final Map<Host, Connection<RelayMessage>> peerToRelayConnections;
	private final Map<Host, Connection<RelayMessage>> otherRelayConnections;
	private final Map<Host, Host> assignedRelayPerPeer;

	private final List<Host> peerList;
	private final int numRelays;
	private final int numProcesses;
	private final int relayID;
	private final Host self;
	private List<Host> relayList;

	public Relay(Properties properties, InputStream hostsConfig, InputStream relayConfig, InputStream latencyConfig) throws IOException {
		this(properties, hostsConfig, latencyConfig);

		relayList = Utils.configToHostList(relayConfig, numRelays);

		assignPeersToRelays();

		try {
			Thread.sleep(CONNECT_RELAYS_WAIT);
		} catch (InterruptedException e) { // this shouldn't be interrupted
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
		connectToRelays();
	}


	public Relay(Properties properties, InputStream hostsConfig, InputStream latencyConfig) throws IOException {

		InetAddress address;
		if (properties.containsKey(ADDRESS_KEY)) address = InetAddress.getByName(properties.getProperty(ADDRESS_KEY));
		else throw new IllegalArgumentException(NAME + " requires binding address");

		int port = Integer.parseInt(properties.getProperty(PORT_KEY, DEFAULT_PORT));
		numRelays = Integer.parseInt(properties.getProperty(NUM_RELAYS, "1"));
		relayID = Integer.parseInt(properties.getProperty(RELAY_ID, "0"));

		int hbInterval = Integer.parseInt(properties.getProperty(HEARTBEAT_INTERVAL_KEY, DEFAULT_HB_INTERVAL));
		int hbTolerance = Integer.parseInt(properties.getProperty(HEARTBEAT_TOLERANCE_KEY, DEFAULT_HB_TOLERANCE));
		int connTimeout = Integer.parseInt(properties.getProperty(CONNECT_TIMEOUT_KEY, DEFAULT_CONNECT_TIMEOUT));

		self = new Host(address, port);

		EventLoopGroup eventExecutors = properties.containsKey(WORKER_GROUP_KEY) ? (EventLoopGroup) properties.get(WORKER_GROUP_KEY) : NetworkManager.createNewWorkerGroup(0);
		RelayMessageSerializer tRelayMessageSerializer = new RelayMessageSerializer();
		network = new NetworkManager<>(tRelayMessageSerializer, this, hbInterval, hbTolerance, connTimeout);
		network.createServerSocket(this, self, this, eventExecutors);

		attributes = new Attributes();
		attributes.putShort(AttributeValidator.CHANNEL_MAGIC_ATTRIBUTE, PROXY_MAGIC_NUMBER);
		attributes.putHost(LISTEN_ADDRESS_ATTRIBUTE, self);

		peerToRelayConnections = new HashMap<>();

		peerToPeerConnections = Collections.newSetFromMap(new ConcurrentHashMap<>());

		disconnectedPeers = Collections.newSetFromMap(new ConcurrentHashMap<>());

		otherRelayConnections = new HashMap<>();
		assignedRelayPerPeer = new HashMap<>();

		relayList = new ArrayList<>();

		numProcesses = Integer.parseInt(properties.getProperty(NUM_PROCESSES, "0"));
		peerList = Utils.configToHostList(hostsConfig, numProcesses);
		Pair<Integer, Integer> range = peerRange(numProcesses, relayID, numRelays);
		latencyMatrix = new ShortMatrix(peerList, latencyConfig, range.getLeft(), range.getRight());
	}

	private static Pair<Integer, Integer> peerRange(int numPeers, int relayID, int numRelays) {
		int r = numPeers % numRelays;
		int size = numPeers / numRelays + ((r > relayID) ? 1 : 0);
		int start = relayID * (numPeers / numRelays) + Math.max(0, Math.min(r, relayID));
		int end = start + size - 1;

		return new ImmutablePair<>(start, end);
	}

	public static void main(String[] args) throws IOException {
		Properties properties = new Properties();
		properties.put(ADDRESS_KEY, args[0]);
		properties.put(PORT_KEY, args[1]);
		properties.put(NUM_PROCESSES, args[2]);
		properties.put(NUM_RELAYS, args[3]);
		properties.put(RELAY_ID, args[4]);


		try (FileInputStream hostsConfig = new FileInputStream(args[5]); FileInputStream relayConfig = new FileInputStream(args[6]); FileInputStream latencyConfig = new FileInputStream(args[7])) {

			new Relay(properties, hostsConfig, relayConfig, latencyConfig);
		}
	}

	private void connectToRelays() {
		for (int i = relayID + 1; i < numRelays; i++) {
			network.createConnection(relayList.get(i), attributes, this);
		}
	}

	private void assignPeersToRelays() {
		for (int i = 0; i < numRelays; i++) {
			Pair<Integer, Integer> range = peerRange(numProcesses, i, numRelays);
			for (int j = range.getLeft(); j <= range.getRight(); j++) {
				assignedRelayPerPeer.put(peerList.get(j), relayList.get(i));
			}
		}
	}

	public void disconnectPeer(Host peer) {
		logger.trace("Disconnecting peer: {}", peer);

		if (!peerToRelayConnections.containsKey(peer)) {
			logger.trace("Disconnecting peer: Peer to kill not connected to relay.");
			return;
		}

		if (!disconnectedPeers.add(peer)) {
			logger.trace("Disconnecting peer: Peer already disconnected.");
			return;
		}

		//send disconnect notification instantly to peer getting disconnected, it is assumed it is connected to this relay
		peerToRelayConnections.get(peer).sendMessage(new RelayPeerDisconnectedMessage(peer, peer, new IOException("Node " + peer + " disconnected")));

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
		logger.trace("Reconnecting peer: {}", peer);

		if (!peerToRelayConnections.containsKey(peer)) {
			logger.trace("Reconnecting peer: Peer to connect not connected to relay.");
			return;
		}

		if (!disconnectedPeers.remove(peer)) logger.trace("Reconnecting peer: Peer already connected.");
		else
			//send signal that peer is reconnected to network
			peerToRelayConnections.get(peer).sendMessage(new RelayPeerDisconnectedMessage(peer, peer, new IOException("Node " + peer + " reconnected")));
	}

	@Override
	public void inboundConnectionUp(Connection<RelayMessage> connection) {
		Host clientSocket;
		try {
			clientSocket = connection.getPeerAttributes().getHost(LISTEN_ADDRESS_ATTRIBUTE);
		} catch (IOException ex) {
			throw new RuntimeException("Error parsing LISTEN_ADDRESS_ATTRIBUTE of inbound connection: " + ex.getMessage());
		}

		if (clientSocket == null) {
			throw new RuntimeException("Inbound connection without LISTEN_ADDRESS: " + connection.getPeer() + " " + connection.getPeerAttributes());
		} else {

			logger.trace("InboundConnectionUp {}", clientSocket);
			Connection<RelayMessage> old;
			if (relayList.contains(clientSocket))
				old = otherRelayConnections.putIfAbsent(clientSocket, connection);
			else
				old = peerToRelayConnections.putIfAbsent(clientSocket, connection);

			if (old != null)
				throw new RuntimeException("Double incoming connection from: " + clientSocket + " (" + connection.getPeer() + ")");
		}
	}

	@Override
	public void inboundConnectionDown(Connection<RelayMessage> connection, Throwable cause) {
		Host clientSocket;
		try {
			clientSocket = connection.getPeerAttributes().getHost(LISTEN_ADDRESS_ATTRIBUTE);
		} catch (IOException ex) {
			throw new RuntimeException("Inbound connection without valid listen address in connectionDown: " + ex.getMessage());
		}

		if (clientSocket == null) {
			throw new RuntimeException("Inbound connection without LISTEN_ADDRESS: " + connection.getPeer() + " " + connection.getPeerAttributes());
		} else {
			throw new RuntimeException("Peer " + clientSocket + " disconnected from relay unexpectedly! cause:" + ((cause == null) ? "" : " " + cause));
		}
	}

	@Override
	public void outboundConnectionUp(Connection<RelayMessage> connection) {
		Host clientSocket = connection.getPeer();

		logger.trace("OutboundConnectionUp {}", clientSocket);
		Connection<RelayMessage> old = this.otherRelayConnections.putIfAbsent(clientSocket, connection);

		if (old != null)
			throw new RuntimeException("Double outgoing connection with: " + clientSocket + " (" + connection.getPeer() + ")");
	}

	@Override
	public void outboundConnectionDown(Connection<RelayMessage> connection, Throwable cause) {
		Host clientSocket;
		try {
			clientSocket = connection.getPeerAttributes().getHost(LISTEN_ADDRESS_ATTRIBUTE);
		} catch (IOException ex) {
			throw new RuntimeException("Outbound connection without valid listen address in connectionDown: " + ex.getMessage());
		}

		if (clientSocket == null) {
			throw new RuntimeException("Outbound connection without LISTEN_ADDRESS: " + connection.getPeer() + " " + connection.getPeerAttributes());
		} else {
			throw new RuntimeException("Relay " + clientSocket + " disconnected unexpectedly! cause:" + ((cause == null) ? "" : " " + cause));
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
			throw new RuntimeException("Server socket bind failed: " + cause);
		}

	}

	public void serverSocketClose(boolean success, Throwable cause) {
		throw new RuntimeException("Server socket closed. " + (success ? "" : "Cause: " + cause));
	}

	@Override
	public void deliverMessage(RelayMessage msg, Connection<RelayMessage> connection) {
		Host from = msg.getFrom();
		Host to = msg.getTo();

		RelayMessage.Type type = msg.getType();

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
			case PEER_DEAD:
				sendMessageWithDelay(msg);
		}
	}

	private void handleConnectionClose(RelayConnectionCloseMessage msg) {
		Host to = msg.getTo();
		Host from = msg.getFrom();

		logger.trace("Connection close message to {} from {}", to, from);

		if (!peerToPeerConnections.remove(new ImmutablePair<>(from, to))) {
			logger.trace("Connection close with no out connection from {} to {}", from, to);
			return;
		}

		sendMessageWithDelay(msg);
	}

	private void handleConnectionAccept(RelayConnectionAcceptMessage msg) {
		Host to = msg.getTo();
		Host from = msg.getFrom();

		logger.trace("Connection accepted message to {} from {}", to, from);

		if (!peerToPeerConnections.contains(new ImmutablePair<>(to, from))) {
			logger.trace("Connection accept with no out connection from {} to {}", from, to);
			return;
		}

		sendMessageWithDelay(msg);
	}

	private void handleAppMessage(RelayAppMessage msg) {
		Host to = msg.getTo();
		Host from = msg.getFrom();

		logger.trace("Message to {} from {}", to, from);

		if (!peerToPeerConnections.contains(new ImmutablePair<>(from, to)) && !peerToPeerConnections.contains(new ImmutablePair<>(to, from))) {
			logger.trace("No connection between {} and {}", from, to);
			return;
		}

		sendMessageWithDelay(msg);
	}

	private void handleConnectionRequest(RelayConnectionOpenMessage msg) {
		Host to = msg.getTo();
		Host from = msg.getFrom();

		logger.trace("Connection request to {} from {}", to, from);

		Pair<Host, Host> pair = new ImmutablePair<>(from, to);
		if (peerToPeerConnections.contains(new ImmutablePair<>(from, to))) {
			logger.trace("Connection request when already existing connection: {}-{}", from, to);
			return;
		}

		sendMessageWithDelay(msg);
		peerToPeerConnections.add(pair);
	}

	@Override
	public boolean validateAttributes(Attributes attr) {
		Short channel = attr.getShort(CHANNEL_MAGIC_ATTRIBUTE);
		return channel != null && channel.equals(PROXY_MAGIC_NUMBER);
	}

	private void sendMessageWithDelay(RelayMessage msg) {
		Host receiver = msg.getTo();

		Host relay = assignedRelayPerPeer.get(receiver);
		if (!relay.equals(self)) {
			otherRelayConnections.get(relay).sendMessage(msg);
			return;
		}

		Connection<RelayMessage> con = peerToRelayConnections.get(receiver);

		Host sender = msg.getFrom();

		Short delay = latencyMatrix.getProperty(sender, receiver);
		if (delay == null) delay = DEFAULT_DELAY;
		con.getLoop().schedule(() -> {
			if (!disconnectedPeers.contains(receiver)) {
				con.sendMessage(msg);
			}
		}, (long) delay - AVERAGE_ERROR, TimeUnit.MILLISECONDS);
	}

}
