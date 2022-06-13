import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import relay.Relay;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import net.sourceforge.argparse4j.inf.ArgumentParser;


import static relay.Relay.*;

public class StartRelay {

	public static void main(String[] args) throws IOException {
		Namespace ns = getNamespace(args);

		Properties properties = new Properties();
		properties.put(ADDRESS_KEY, ns.getString("address"));
		properties.put(PORT_KEY, args[1]);
		properties.put(NUM_PEERS, args[2]);
		properties.put(NUM_RELAYS, args[3]);
		properties.put(RELAY_ID, args[4]);


		try (FileInputStream hostsConfig = new FileInputStream(args[5]); FileInputStream relayConfig = new FileInputStream(args[6]); FileInputStream latencyConfig = new FileInputStream(args[7])) {

			new Relay(properties, hostsConfig, relayConfig, latencyConfig);
		}
	}

	private static Namespace getNamespace(String[] args) throws UnknownHostException {
		ArgumentParser parser = ArgumentParsers.newFor("Relay").build().defaultHelp(true);
		parser.addArgument("nodes").type(int.class).help("number of nodes");
		parser.addArgument("relays").type(int.class).setDefault(1).help("number of relays");
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
