package relay;

import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pt.unl.fct.di.novasys.network.AttributeValidator;
import pt.unl.fct.di.novasys.network.Connection;
import pt.unl.fct.di.novasys.network.NetworkManager;
import pt.unl.fct.di.novasys.network.data.Attributes;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.network.listeners.InConnListener;
import pt.unl.fct.di.novasys.network.listeners.MessageListener;

import relay.messaging.*;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

public class Relay implements InConnListener<RelayMessage>, MessageListener<RelayMessage>, AttributeValidator {

    private static final Logger logger = LogManager.getLogger(Relay.class);
    private static final Short PROXY_MAGIC_NUMBER = 0x1369;

    public static final String NAME = "Relay";

    public static final String ADDRESS_KEY = "address";
    public static final String PORT_KEY = "port";
    public static final String HEARTBEAT_INTERVAL_KEY = "heartbeat_interval";
    public static final String HEARTBEAT_TOLERANCE_KEY = "heartbeat_tolerance";
    public static final String CONNECT_TIMEOUT_KEY = "connect_timeout";
    public static final String WORKER_GROUP_KEY = "workerGroup";

    public static final String LISTEN_ADDRESS_ATTRIBUTE = "listen_address";

    public static final String DEFAULT_PORT = "9082";
    public static final String DEFAULT_HB_INTERVAL = "0";
    public static final String DEFAULT_HB_TOLERANCE = "0";
    public static final String DEFAULT_CONNECT_TIMEOUT = "1000";

    public static final short DEFAULT_DELAY = 0;

    private Attributes attributes;

    private final NetworkManager<RelayMessage> network;

    private final Map<Host, Connection<RelayMessage>> peerToRelayConnections;

    private final Map<Host, Set<Host>> peerToPeerOutConnections;
    private final Map<Host, Set<Host>> peerToPeerInConnections;

    private final Set<Host> deadPeers;

    private final Map<Host, EventLoop> loopPerReceiver;

    //there's probably a better existing structure for this
    private Map<Host,Map<Host,Short>> trafficMatrix;

    public Relay(Properties properties, Map<Host,Map<Host,Short>> trafficMatrix) throws IOException {
        this(properties);
        this.trafficMatrix = trafficMatrix;
    }

    public Relay(Properties properties, InputStream hostsConfig, InputStream delayConfig) throws IOException {
        this(properties);

        //initialize traffic matrix ######
        trafficMatrix = new HashMap<>();

        BufferedReader reader = new BufferedReader(new InputStreamReader(hostsConfig));

        LinkedList<Host> hostList = new LinkedList<>();
        reader.lines().forEach((String line) -> {
        String[] parts = line.split(":");
            try {
               InetAddress ip = InetAddress.getByName(parts[0]);
               int port = Integer.parseInt(parts[1]);

               hostList.add(new Host(ip,port));
            } catch (UnknownHostException e) {
               logger.error("Bad address in hosts config.");
               System.exit(1);
            }
        });

        reader = new BufferedReader(new InputStreamReader(delayConfig));
        for (Host hostI: hostList) {
            String[] strValues = reader.readLine().split(" ");
            int i = 0;
            trafficMatrix.put(hostI, new HashMap<>());
            for (Host hostJ: hostList) {
                short delay = Short.parseShort(strValues[i++]);
                trafficMatrix.get(hostI).put(hostJ, delay);
            }
        }
        //######
    }

    public Relay(Properties properties) throws IOException {
        InetAddress addr;
        if (properties.containsKey(ADDRESS_KEY))
            addr = InetAddress.getByName(properties.getProperty(ADDRESS_KEY));
        else
            throw new IllegalArgumentException(NAME + " requires binding address");

        int port = Integer.parseInt(properties.getProperty(PORT_KEY, DEFAULT_PORT));
        int hbInterval = Integer.parseInt(properties.getProperty(HEARTBEAT_INTERVAL_KEY, DEFAULT_HB_INTERVAL));
        int hbTolerance = Integer.parseInt(properties.getProperty(HEARTBEAT_TOLERANCE_KEY, DEFAULT_HB_TOLERANCE));
        int connTimeout = Integer.parseInt(properties.getProperty(CONNECT_TIMEOUT_KEY, DEFAULT_CONNECT_TIMEOUT));

        Host listenAddress = new Host(addr, port);

        EventLoopGroup eventExecutors = properties.containsKey(WORKER_GROUP_KEY) ?
                (EventLoopGroup) properties.get(WORKER_GROUP_KEY) :
                NetworkManager.createNewWorkerGroup(0);
        RelayMessageSerializer tRelayMessageSerializer = new RelayMessageSerializer();
        network = new NetworkManager<>(tRelayMessageSerializer, this, hbInterval, hbTolerance, connTimeout);
        network.createServerSocket(this, listenAddress, this, eventExecutors);

        attributes = new Attributes();
        attributes.putShort(AttributeValidator.CHANNEL_MAGIC_ATTRIBUTE, PROXY_MAGIC_NUMBER);
        attributes.putHost(LISTEN_ADDRESS_ATTRIBUTE, listenAddress);

        peerToRelayConnections = new ConcurrentHashMap<>();

        peerToPeerOutConnections = new ConcurrentHashMap<>();
        peerToPeerInConnections = new ConcurrentHashMap<>();

        deadPeers = new ConcurrentSkipListSet<>();

        loopPerReceiver = new ConcurrentHashMap<>();
    }

    public void killPeer(Host peer) {
        logger.debug("Killing peer: "+peer);

        if(!peerToRelayConnections.containsKey(peer)) {
            logger.debug("Peer to kill not connected to relay.");
            return;
        }

        if(!deadPeers.add(peer)) {
            logger.debug("Peer already dead.");
            return;
        }

        for (Host connectedPeer : peerToPeerOutConnections.keySet()) {
            Set<Host> outConnections = peerToPeerOutConnections.get(connectedPeer);
            if (outConnections.remove(peer)) {
                sendMessageWithDelay(new RelayPeerDeadMessage(peer, connectedPeer, new Throwable("Node " + peer + " died")), peerToRelayConnections.get(connectedPeer));
            }
        }

        for (Host connectedPeer : peerToPeerInConnections.keySet()) {
            Set<Host> inConnections = peerToPeerInConnections.get(connectedPeer);
            if (inConnections.remove(peer)) {
                sendMessageWithDelay(new RelayPeerDeadMessage(peer, connectedPeer, new Throwable("Node " + peer + " died")), peerToRelayConnections.get(connectedPeer));
            }
        }

        peerToPeerOutConnections.put(peer, new HashSet<>());
        peerToPeerInConnections.put(peer, new HashSet<>());
    }

    public void connectPeer(Host peer) {
        logger.debug("Connecting peer: "+ peer);

        if (!peerToRelayConnections.containsKey(peer)) {
            logger.error("Peer to connect not connected to relay.");
            return;
        }

        if(!deadPeers.remove(peer))
            logger.debug("Peer already connected.");

    }

    @Override
    public void inboundConnectionUp(Connection<RelayMessage> connection) {
        Host clientSocket;
        try {
            clientSocket = connection.getPeerAttributes().getHost("listen_address");
        } catch (IOException ex) {
            logger.error("Error parsing LISTEN_ADDRESS_ATTRIBUTE of inbound connection: " + ex.getMessage());
            connection.disconnect();
            return;
        }

        if (clientSocket == null) {
            logger.error("Inbound connection without LISTEN_ADDRESS: " + connection.getPeer() + " " + connection.getPeerAttributes());
        } else {
            logger.debug("InboundConnectionUp " + clientSocket);
            Connection<RelayMessage> old = this.peerToRelayConnections.putIfAbsent(clientSocket, connection);

            if (old != null)
                throw new RuntimeException("Double incoming connection from: " + clientSocket + " (" + connection.getPeer() + ")");

            peerToPeerOutConnections.put(clientSocket, new HashSet<>());
            peerToPeerInConnections.put(clientSocket, new HashSet<>());

            loopPerReceiver.put(clientSocket, new DefaultEventLoop());
        }
    }

    @Override
    public void inboundConnectionDown(Connection<RelayMessage> connection, Throwable throwable) {
        Host clientSocket;
        try {
            clientSocket = connection.getPeerAttributes().getHost("listen_address");
        } catch (IOException ex) {
            logger.error("Inbound connection without valid listen address in connectionDown: " + ex.getMessage());
            connection.disconnect();
            return;
        }

        if (clientSocket == null) {
            logger.error("Inbound connection without LISTEN_ADDRESS: " + connection.getPeer() + " " + connection.getPeerAttributes());
        } else {
            logger.error("Peer "+clientSocket+" disconnected from relay unexpectedly! cause:"+((throwable == null) ? "" : " "+throwable));
            peerToRelayConnections.remove(clientSocket);
        }
    }

    public void serverSocketBind(boolean success, Throwable cause) {
        if (success) {
            logger.debug("Server socket ready");
        } else {
            logger.error("Server socket bind failed: " + cause);
        }

    }

    public void serverSocketClose(boolean success, Throwable cause) {
        logger.debug("Server socket closed. " + (success ? "" : "Cause: " + cause));
    }

    @Override
    public void deliverMessage(RelayMessage msg, Connection<RelayMessage> connection) {
        Host from = msg.getFrom();
        Host to = msg.getTo();
        RelayMessage.Type type = msg.getType();

        Connection<RelayMessage> con = peerToRelayConnections.get(to);
        if(con == null) {
            logger.error("No relay connection to " + to + " but captured message directed to it");
            return;
        }

        if(deadPeers.contains(from))
            return;

        if(deadPeers.contains(to)) {
            if(type == RelayMessage.Type.CONN_OPEN)
                sendMessageWithDelay(new RelayConnectionFailMessage(to, from, new Throwable("Peer "+to+" is dead.")), peerToRelayConnections.get(from));
            return;
        }

        switch (type) {
            case APP_MSG: handleAppMessage((RelayAppMessage) msg, from, to, con); break;
            case CONN_OPEN: handleConnectionRequest((RelayConnectionOpenMessage) msg, from, to, con); break;
            case CONN_CLOSE: handleConnectionClose((RelayConnectionCloseMessage) msg, from, to, con); break;
            case CONN_ACCEPT: handleConnectionAccept((RelayConnectionAcceptMessage) msg, from, to, con); break;
            case CONN_FAIL:
            case PEER_DEAD: throw new AssertionError("Got "+type.name()+" in single relay implementation");
        }
    }

    private void handleConnectionClose(RelayConnectionCloseMessage msg, Host from, Host to, Connection<RelayMessage> conTo) {
        logger.debug("Connection close message to "+to+" from "+from);

        if(!peerToPeerOutConnections.get(from).remove(to)) {
            logger.error("Connection close with no out connection from " + from + " to " + to);
            return;
        }
        peerToPeerInConnections.get(to).remove(from);

        sendMessageWithDelay(msg, conTo);
    }

    private void handleConnectionAccept(RelayConnectionAcceptMessage msg, Host from, Host to, Connection<RelayMessage> conTo) {
        logger.debug("Connection accepted message to "+to+" from "+from);

        if(!peerToPeerOutConnections.get(to).contains(from)) {
            logger.error("Connection accept with no out connection from " + from + " to " + to);
            return;
        }

        sendMessageWithDelay(msg, conTo);
    }

    private void handleAppMessage(RelayAppMessage msg, Host from, Host to, Connection<RelayMessage> conTo) {
        logger.debug("Message to "+to+" from "+from);

        if(!peerToPeerOutConnections.get(from).contains(to) && !peerToPeerOutConnections.get(to).contains(from)) {
            logger.error("No connection between "+from+" and "+to);
            return;
        }

        sendMessageWithDelay(msg, conTo);
    }

    private void handleConnectionRequest(RelayConnectionOpenMessage msg, Host from, Host to, Connection<RelayMessage> conTo) {
        logger.debug("Connection request to "+to+" from "+from);

        if(peerToPeerOutConnections.get(from).contains(to)) {
            logger.error("Connection request when already existing connection: " + from + "-" + to);
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

        short delay;
        try {
            delay = trafficMatrix.get(sender).get(receiver);
        } catch (NullPointerException ex) {
            //delay not defined for pair in matrix or matrix not defined
            delay = DEFAULT_DELAY;
        }

        EventLoop loop = loopPerReceiver.get(receiver);
        loop.schedule(() -> {
            if(!deadPeers.contains(receiver))
                con.sendMessage(msg);

        }, delay, TimeUnit.MILLISECONDS);
    }
}
