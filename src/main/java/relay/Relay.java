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
import java.util.concurrent.ConcurrentSkipListSet;
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
	private static final Short AVERAGE_ERROR = 0;

	private static final Logger logger = LogManager.getLogger(Relay.class);
	private static final Short PROXY_MAGIC_NUMBER = 0x1369;


	private final NetworkManager<RelayMessage> network;
	private final Attributes attributes;

	private final Map<Host, Connection<RelayMessage>> peerToRelayConnections;

	private final Map<Host, Set<Host>> peerToPeerOutConnections;
	private final Map<Host, Set<Host>> peerToPeerInConnections;

	private final Set<Host> disconnectedPeers;

	private final ShortMatrix latencyMatrix;

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

		relayList = Utils.configToHostList(relayConfig);

		assignPeersToRelays();

		try {
			Thread.sleep(5000);
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

		peerToPeerOutConnections = new ConcurrentHashMap<>();
		peerToPeerInConnections = new ConcurrentHashMap<>();

		disconnectedPeers = new ConcurrentSkipListSet<>();

		otherRelayConnections = new HashMap<>();
		assignedRelayPerPeer = new HashMap<>();

		relayList = new ArrayList<>();
		peerList = Utils.configToHostList(hostsConfig);

		numProcesses = Integer.parseInt(properties.getProperty(NUM_PROCESSES, String.valueOf(peerList.size())));

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

		sendPeerDisconnectNotifications(peer, peerToPeerOutConnections);

		sendPeerDisconnectNotifications(peer, peerToPeerInConnections);

		peerToPeerOutConnections.put(peer, new HashSet<>());
		peerToPeerInConnections.put(peer, new HashSet<>());
	}

	private void sendPeerDisconnectNotifications(Host peer, Map<Host, Set<Host>> peerToPeerConnections) {
		for (Map.Entry<Host, Set<Host>> entry : peerToPeerConnections.entrySet()) {
			if (entry.getKey().equals(peer)) continue;

			Set<Host> inConnections = entry.getValue();
			if (inConnections.remove(peer)) {
				RelayMessage msg = new RelayPeerDisconnectedMessage(peer, entry.getKey(), new IOException("Node " + peer + " disconnected"));
				Host relay = assignedRelayPerPeer.get(peer);
				if (!relay.equals(self)) otherRelayConnections.get(relay).sendMessage(msg);
				else sendMessageWithDelay(msg, peerToRelayConnections.get(entry.getKey()));
			}
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
			clientSocket = connection.getPeerAttributes().getHost("listen_address");
		} catch (IOException ex) {
			throw new RuntimeException("Error parsing LISTEN_ADDRESS_ATTRIBUTE of inbound connection: " + ex.getMessage());
		}

		if (clientSocket == null) {
			throw new RuntimeException("Inbound connection without LISTEN_ADDRESS: " + connection.getPeer() + " " + connection.getPeerAttributes());
		} else {

			logger.trace("InboundConnectionUp {}", clientSocket);
			Connection<RelayMessage> old;
			if (relayList.contains(clientSocket)) old = otherRelayConnections.putIfAbsent(clientSocket, connection);
			else {
				old = peerToRelayConnections.putIfAbsent(clientSocket, connection);
				peerToPeerOutConnections.put(clientSocket, new HashSet<>());
				peerToPeerInConnections.put(clientSocket, new HashSet<>());
			}

			if (old != null)
				throw new RuntimeException("Double incoming connection from: " + clientSocket + " (" + connection.getPeer() + ")");
		}
	}

	@Override
	public void inboundConnectionDown(Connection<RelayMessage> connection, Throwable cause) {
		Host clientSocket;
		try {
			clientSocket = connection.getPeerAttributes().getHost("listen_address");
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
		Host clientSocket;
		try {
			clientSocket = connection.getPeerAttributes().getHost("listen_address");
		} catch (IOException ex) {
			throw new RuntimeException("Error parsing LISTEN_ADDRESS_ATTRIBUTE of inbound connection: " + ex.getMessage());
		}

		if (clientSocket == null) {
			throw new RuntimeException("Outbound connection without LISTEN_ADDRESS: " + connection.getPeer() + " " + connection.getPeerAttributes());
		} else {
			logger.trace("OutboundConnectionUp {}", clientSocket);
			Connection<RelayMessage> old = this.otherRelayConnections.putIfAbsent(clientSocket, connection);

			if (old != null)
				throw new RuntimeException("Double outgoing connection with: " + clientSocket + " (" + connection.getPeer() + ")");
		}
	}

	@Override
	public void outboundConnectionDown(Connection<RelayMessage> connection, Throwable cause) {
		Host clientSocket;
		try {
			clientSocket = connection.getPeerAttributes().getHost("listen_address");
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

		Host relay = assignedRelayPerPeer.get(to);
		if (!relay.equals(self)) {
			//redirect to relay responsible for peer
			otherRelayConnections.get(relay).sendMessage(msg);
			return;
		}

		RelayMessage.Type type = msg.getType();

		Connection<RelayMessage> con = peerToRelayConnections.get(to);
		if (con == null) {
			logger.trace("No relay connection to {} but captured message directed to it", to);
			return;
		}

		if (disconnectedPeers.contains(to)) {
			if (type == RelayMessage.Type.CONN_OPEN) {
				RelayMessage failMessage = new RelayConnectionFailMessage(to, from, new IOException("Peer " + to + " is disconnected."));
				relay = assignedRelayPerPeer.get(from);
				if (relay.equals(self)) sendMessageWithDelay(failMessage, peerToRelayConnections.get(from));
				else otherRelayConnections.get(relay).sendMessage(failMessage);
			}
			return;
		}

		switch (type) {
			case APP_MSG:
				handleAppMessage((RelayAppMessage) msg, from, to, con);
				break;
			case CONN_OPEN:
				handleConnectionRequest((RelayConnectionOpenMessage) msg, from, to, con);
				break;
			case CONN_CLOSE:
				handleConnectionClose((RelayConnectionCloseMessage) msg, from, to, con);
				break;
			case CONN_ACCEPT:
				handleConnectionAccept((RelayConnectionAcceptMessage) msg, from, to, con);
				break;
			case CONN_FAIL:
			case PEER_DEAD:
				sendMessageWithDelay(msg, con);
		}
	}

	private void handleConnectionClose(RelayConnectionCloseMessage msg, Host from, Host to, Connection<RelayMessage> conTo) {
		logger.trace("Connection close message to {} from {}", to, from);

		if (!peerToPeerOutConnections.get(from).remove(to)) {
			logger.trace("Connection close with no out connection from {} to {}", from, to);
			return;
		}
		peerToPeerInConnections.get(to).remove(from);

		sendMessageWithDelay(msg, conTo);
	}

	private void handleConnectionAccept(RelayConnectionAcceptMessage msg, Host from, Host to, Connection<RelayMessage> conTo) {
		logger.trace("Connection accepted message to {} from {}", to, from);

		if (!peerToPeerOutConnections.get(to).contains(from)) {
			logger.trace("Connection accept with no out connection from {} to {}", from, to);
			return;
		}

		sendMessageWithDelay(msg, conTo);
	}

	private void handleAppMessage(RelayAppMessage msg, Host from, Host to, Connection<RelayMessage> conTo) {
		logger.trace("Message to {} from {}", to, from);

		if (!peerToPeerOutConnections.get(from).contains(to) && !peerToPeerOutConnections.get(to).contains(from)) {
			logger.trace("No connection between {} and {}", from, to);
			return;
		}

		sendMessageWithDelay(msg, conTo);
	}

	private void handleConnectionRequest(RelayConnectionOpenMessage msg, Host from, Host to, Connection<RelayMessage> conTo) {
		logger.trace("Connection request to {} from {}", to, from);

		if (peerToPeerOutConnections.get(from).contains(to)) {
			logger.trace("Connection request when already existing connection: {}-{}", from, to);
			return;
		}

		sendMessageWithDelay(msg, conTo);
		peerToPeerOutConnections.get(from).add(to);
		peerToPeerInConnections.get(to).add(from);
	}

	@Override
	public boolean validateAttributes(Attributes attr) {
		Short channel = attr.getShort(CHANNEL_MAGIC_ATTRIBUTE);
		return channel != null && channel.equals(PROXY_MAGIC_NUMBER);
	}

	private void sendMessageWithDelay(RelayMessage msg, Connection<RelayMessage> con) {
		Host sender = msg.getFrom();
		Host receiver = msg.getTo();

		Short delay = latencyMatrix.getProperty(sender, receiver);
		if (delay == null) delay = DEFAULT_DELAY;
		con.getLoop().schedule(() -> {
			if (!disconnectedPeers.contains(receiver)) {
				con.sendMessage(msg);
			}
		}, (long) delay - AVERAGE_ERROR, TimeUnit.MILLISECONDS);
	}

}
