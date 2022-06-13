import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import relay.Relay;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import static relay.Relay.*;

public class StartRelay {

	public static void main(String[] args) throws IOException {
		Namespace ns = getNamespace(args);

		Properties properties = new Properties();
		properties.put(ADDRESS_KEY, ns.getString("address"));
		properties.put(PORT_KEY, ns.getInt("port"));
		properties.put(NUM_NODES, ns.getInt("nodes"));
		properties.put(NUM_RELAYS, ns.getInt("relays"));
		properties.put(RELAY_ID, ns.getInt("relay_id"));


		try (FileInputStream hostsConfig = new FileInputStream(ns.getString("list_nodes"));
			 FileInputStream relayConfig = new FileInputStream(ns.getString("list_relays"));
			 FileInputStream latencyConfig = new FileInputStream(ns.getString("latency_matrix"))) {

			new Relay(properties, hostsConfig, relayConfig, latencyConfig);
		}
	}

	private static Namespace getNamespace(String[] args) throws UnknownHostException {
		ArgumentParser parser = ArgumentParsers.newFor("Relay").build().defaultHelp(true);
		parser.addArgument("nodes").type(int.class).help("number of nodes");
		parser.addArgument("relays").type(int.class).nargs("?").setDefault(1).help("number of relays");
		parser.addArgument("relay_id").type(int.class).nargs("?").setDefault(0).help("relay ID");
		parser.addArgument("list_nodes").help("file with node list");
		parser.addArgument("list_relays").help("file with relay list");
		parser.addArgument("-a", "--address").setDefault(InetAddress.getLocalHost().getHostAddress()).help("local private address");
		parser.addArgument("-p", "--port").type(int.class).setDefault(9082).help("relay port");
		parser.addArgument("-lm", "--latency_matrix").help("file with latency matrix");

		try {
			return parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			System.exit(1);
		}

		return null;
	}
}
