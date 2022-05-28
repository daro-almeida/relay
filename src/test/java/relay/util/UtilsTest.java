package relay.util;

import pt.unl.fct.di.novasys.network.data.Host;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UtilsTest {

	private InputStream hostsConfig;

	@org.junit.jupiter.api.BeforeEach
	void beforeEach() throws IOException {
		String str = "127.0.0.1:5000\n" +
				"127.0.0.1:5001\n" +
				"127.0.0.1:5002\n" +
				"127.0.0.1:5003\n" +
				"127.0.0.1:5004\n" +
				"127.0.0.1:5005\n" +
				"127.0.0.1:5006\n" +
				"127.0.0.1:5007\n" +
				"127.0.0.1:5008\n" +
				"127.0.0.1:5009";
		hostsConfig = new ByteArrayInputStream(str.getBytes());
	}

	@org.junit.jupiter.api.Test
	void configToHostList() throws UnknownHostException {
		List<Host> hosts = Utils.configToHostList(hostsConfig, 5);

		List<Host> expected = new ArrayList<Host>() {
			{
				add(new Host(InetAddress.getByName("localhost"), 5000));
				add(new Host(InetAddress.getByName("localhost"), 5001));
				add(new Host(InetAddress.getByName("localhost"), 5002));
				add(new Host(InetAddress.getByName("localhost"), 5003));
				add(new Host(InetAddress.getByName("localhost"), 5004));
			}
		};

		assertEquals(expected, hosts);
	}
}