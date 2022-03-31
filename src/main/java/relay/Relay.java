package relay;

import io.netty.channel.EventLoopGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.channel.proxy.messaging.*;
import pt.unl.fct.di.novasys.network.AttributeValidator;
import pt.unl.fct.di.novasys.network.Connection;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.NetworkManager;
import pt.unl.fct.di.novasys.network.data.Attributes;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.network.listeners.InConnListener;
import pt.unl.fct.di.novasys.network.listeners.MessageListener;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Relay<T> implements InConnListener<ProxyMessage<T>>, MessageListener<ProxyMessage<T>>, AttributeValidator {

    private static final Logger logger = LogManager.getLogger(Relay.class);
    private static final Short PROXY_MAGIC_NUMBER = 0x1369;

    public final static String NAME = "Relay";

    public final static String ADDRESS_KEY = "address";
    public final static String PORT_KEY = "port";
    public final static String HEARTBEAT_INTERVAL_KEY = "heartbeat_interval";
    public final static String HEARTBEAT_TOLERANCE_KEY = "heartbeat_tolerance";
    public final static String CONNECT_TIMEOUT_KEY = "connect_timeout";
    public final static String WORKER_GROUP_KEY = "workerGroup";

    public static final String LISTEN_ADDRESS_ATTRIBUTE = "listen_address";

    public final static String DEFAULT_PORT = "9082";
    public final static String DEFAULT_HB_INTERVAL = "0";
    public final static String DEFAULT_HB_TOLERANCE = "0";
    public final static String DEFAULT_CONNECT_TIMEOUT = "1000";

    private Attributes attributes;

    private final NetworkManager<ProxyMessage<T>> network;

    private final Map<Host, Connection<ProxyMessage<T>>> peerToRelayConnections;

    private final Map<Host, Set<Host>> peerToPeerOutConnections;

    public Relay(ISerializer<T> serializer, Properties properties) throws IOException {
        InetAddress addr;
        if (properties.containsKey(ADDRESS_KEY))
            addr = Inet4Address.getByName(properties.getProperty(ADDRESS_KEY));
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
        ProxyMessageSerializer<T> tProxyMessageSerializer = new ProxyMessageSerializer<>(serializer);
        network = new NetworkManager<>(tProxyMessageSerializer, this, hbInterval, hbTolerance, connTimeout);
        network.createServerSocket(this, listenAddress, this, eventExecutors);

        attributes = new Attributes();
        attributes.putShort(AttributeValidator.CHANNEL_MAGIC_ATTRIBUTE, PROXY_MAGIC_NUMBER);
        attributes.putHost(LISTEN_ADDRESS_ATTRIBUTE, listenAddress);

        peerToRelayConnections = new ConcurrentHashMap<>();
        peerToPeerOutConnections = new ConcurrentHashMap<>();

    }

    public Relay(Properties properties) throws IOException {
        this(null, properties);
    }

    @Override
    public void inboundConnectionUp(Connection<ProxyMessage<T>> connection) {
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
            Connection<ProxyMessage<T>> old;

            old = this.peerToRelayConnections.putIfAbsent(clientSocket, connection);

            if (old != null) {
                throw new RuntimeException("Double incoming connection from: " + clientSocket + " (" + connection.getPeer() + ")");
            }

            peerToPeerOutConnections.put(clientSocket, new HashSet<>());
        }
    }

    @Override
    public void inboundConnectionDown(Connection<ProxyMessage<T>> connection, Throwable throwable) {

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
    public void deliverMessage(ProxyMessage<T> msg, Connection<ProxyMessage<T>> connection) {
        Host from = msg.getFrom();
        Host to = msg.getTo();
        ProxyMessage.Type type = msg.getType();

        //logger.debug(type.name() + " message from " + from + " to "+ to);

        Connection<ProxyMessage<T>> con = peerToRelayConnections.get(to);
        if(con == null) {
            logger.debug("No relay connection to "+to);
            return;
        }

        switch (type) {
            case APP_MSG: handleAppMessage((ProxyAppMessage<T>) msg, from, to, con); break;
            case CONN_OPEN: handleConnectionRequest((ProxyConnectionOpenMessage<T>) msg, from, to, con); break;
            case CONN_CLOSE: handleConnectionClose((ProxyConnectionCloseMessage<T>) msg, from, to, con); break;
            case CONN_ACCEPT: handleConnectionAccept((ProxyConnectionAcceptMessage<T>) msg, from, to, con); break;
            case CONN_FAIL:
        }
    }

    private void handleConnectionClose(ProxyConnectionCloseMessage<T> msg, Host from, Host to, Connection<ProxyMessage<T>> con) {
        logger.debug("Connection close message to "+to+" from "+from);

        if(!peerToPeerOutConnections.get(to).contains(from)) {
            logger.error("Connection close with no out connection from " + from + " to " + to);
            return;
        }
    }

    private void handleConnectionAccept(ProxyConnectionAcceptMessage<T> msg, Host from, Host to, Connection<ProxyMessage<T>> conTo) {
        logger.debug("Connection accepted message to "+to+" from "+from);

        if(!peerToPeerOutConnections.get(to).contains(from)) {
            logger.error("Connection accept with no out connection from " + from + " to " + to);
            return;
        }

        conTo.sendMessage(msg);
    }

    private void handleAppMessage(ProxyAppMessage<T> msg, Host from, Host to, Connection<ProxyMessage<T>> conTo) {
        logger.debug("Message "+msg.getPayload()+" to "+to+" from "+from);

        if(!peerToPeerOutConnections.get(from).contains(to) && !peerToPeerOutConnections.get(to).contains(from)) {
            logger.error("No connection between "+from+" and "+to);
            return;
        }

        conTo.sendMessage(msg);
    }

    private void handleConnectionRequest(ProxyConnectionOpenMessage<T> msg, Host from, Host to, Connection<ProxyMessage<T>> conTo) {
        logger.debug("Connection request to "+to+" from "+from);

        if(peerToPeerOutConnections.get(from).contains(to)) {
            logger.error("Connection request when already existing connection: " + from + "-" + to);
            return;
        }

        conTo.sendMessage(msg);
        peerToPeerOutConnections.get(from).add(to);
    }

    @Override
    public boolean validateAttributes(Attributes attr) {
        Short channel = attr.getShort(CHANNEL_MAGIC_ATTRIBUTE);
        return channel != null && channel.equals(PROXY_MAGIC_NUMBER);
    }
}
