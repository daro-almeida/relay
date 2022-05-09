package relay.util;

import pt.unl.fct.di.novasys.network.data.Host;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

public class Utils {

    public static List<Host> configToHostList(InputStream hostsConfig) throws UnknownHostException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(hostsConfig));

        LinkedList<Host> hostList = new LinkedList<>();
        reader.lines().forEach((String line) -> {
            String[] parts = line.split(":");

            InetAddress ip;
            try {
                ip = InetAddress.getByName(parts[0]);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }

            int port = Integer.parseInt(parts[1]);

            hostList.add(new Host(ip,port));
        });

        return hostList;
    }
}
