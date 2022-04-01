import pt.unl.fct.di.novasys.channel.proxy.ProxyChannel;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

public class StartPeer {

    static {
        System.setProperty("log4j.configurationFile", "log4j2.xml");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Host self = new Host(InetAddress.getByName("localhost"), Short.parseShort(args[0]));

        Properties properties = new Properties();
        properties.put("address","localhost");
        properties.put("port",String.valueOf(self.getPort()));
        properties.put("relay_address","localhost");
        properties.put("relay_port",args[1]);
        properties.put("trigger_sent","true");

        ProxyChannel<String> ch = new ProxyChannel<>(new StringSerializer(), new SimpleListener<>(self), properties);

        Thread.sleep(10000);

        Host target = new Host(InetAddress.getByName("localhost"),Integer.parseInt(args[2]));
        ch.openConnection(target);
        while(true) {
            Thread.sleep((long) (1000 + Math.random() * 4000));
            ch.sendMessage("Hello!", target, ProxyChannel.CONNECTION_OUT);
        }

    }
}
