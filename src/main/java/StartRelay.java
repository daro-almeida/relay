import relay.Relay;

import java.io.IOException;
import java.util.Properties;

public class StartRelay {

    static {
        System.setProperty("log4j.configurationFile", "log4j2.xml");
    }

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.put("address","localhost");
        properties.put("port",args[0]);

        Relay<String> relay = new Relay<>(new StringSerializer(), properties);

    }
}
