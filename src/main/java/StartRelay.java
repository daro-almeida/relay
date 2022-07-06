import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import relay.BWLatencyRelay;
import relay.Relay;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static relay.Relay.*;

public class StartRelay {

	static {
		System.setProperty("log4j.configurationFile", "log4j2.xml");
	}

	public static void main(String[] args) throws IOException {
		Namespace ns = getNamespace(args);

		Properties properties = new Properties();
		properties.put(ADDRESS_KEY, ns.getString("address"));
		properties.put(PORT_KEY, ns.getInt("port").toString());
		properties.put(NUM_NODES, ns.getInt("nodes").toString());
		properties.put(NUM_RELAYS, ns.getInt("relays").toString());
		properties.put(RELAY_ID, ns.getInt("relay_id").toString());
		properties.put(SLEEP, ns.getInt("sleep").toString());

		InputStream hostsConfig = Files.newInputStream(Paths.get(ns.getString("list_nodes")));
		InputStream relaysConfig = Files.newInputStream(Paths.get(ns.getString("list_relays")));
		InputStream latencyConfig = Files.newInputStream(Paths.get(ns.getString("latency_matrix")));

		Relay relay;
		if (ns.getString("bandwidth_config") == null)
			relay = new Relay(properties, hostsConfig, relaysConfig, latencyConfig);
		else {
			InputStream bandwidthConfig = Files.newInputStream(Paths.get(ns.getString("bandwidth_config")));
			relay = new BWLatencyRelay(properties, hostsConfig, relaysConfig, latencyConfig, bandwidthConfig);
		}

//		if (ns.getString("events_config") == null) {
//			new PeerEvents(relay, eventsConfig);
//		}

	}

	private static Namespace getNamespace(String[] args) throws UnknownHostException {
		ArgumentParser parser = ArgumentParsers.newFor("Relay").build().defaultHelp(true);
		parser.addArgument("nodes").type(Integer.class).help("number of nodes");
		parser.addArgument("relays").type(Integer.class).nargs("?").setDefault(1).help("number of relays");
		parser.addArgument("relay_id").type(Integer.class).nargs("?").setDefault(0).help("relay ID");
		parser.addArgument("list_nodes").help("file with node list");
		parser.addArgument("list_relays").help("file with relay list");
		parser.addArgument("-a", "--address").setDefault(InetAddress.getLocalHost().getHostAddress()).help("local private address");
		parser.addArgument("-p", "--port").type(Integer.class).setDefault(9082).help("relay port");
		parser.addArgument("-lm", "--latency_matrix").help("file with latency matrix");
		parser.addArgument("-bc", "--bandwidth_config").help("file with bandwidth config for nodes");
		parser.addArgument("-ec", "--events_config").help("file with scheduled events");
		parser.addArgument("-s", "--sleep").type(Integer.class).setDefault(4000).help("sleep time in ms before connecting to other relays");

		try {
			return parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			System.exit(1);
		}

		return null;
	}
}
