package relay.util;

import pt.unl.fct.di.novasys.network.data.Host;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Utils {

	private Utils() {
	}

	public static List<Host> configToHostList(InputStream hostsConfig) {
		return configToHostList(hostsConfig, Integer.MAX_VALUE);
	}

	public static List<Host> configToHostList(InputStream hostsConfig, int numHosts) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(hostsConfig));

		List<Host> hostList;
		if (numHosts < Integer.MAX_VALUE)
			hostList = new ArrayList<>(numHosts);
		else
			hostList = new ArrayList<>();
		final int[] i = {0};
		reader.lines().forEach((String line) -> {
			if (i[0] >= numHosts) return; //break

			String[] parts = line.split(":");

			InetAddress ip;
			try {
				ip = InetAddress.getByName(parts[0]);
			} catch (UnknownHostException e) {
				throw new RuntimeException(e);
			}

			int port = Integer.parseInt(parts[1]);

			hostList.add(new Host(ip, port));

			i[0]++;
		});

		return hostList;
	}
}
