import pt.unl.fct.di.novasys.network.data.Host;
import relay.Relay;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

public class StartRelay {

    static {
        System.setProperty("log4j.configurationFile", "log4j2.xml");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties properties = new Properties();
        properties.put("address","localhost");
        properties.put("port",args[0]);

        Relay relay = new Relay(properties);
    }
}
