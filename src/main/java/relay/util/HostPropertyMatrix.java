package relay.util;

import pt.unl.fct.di.novasys.network.data.Host;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class HostPropertyMatrix<T> {

    private final Map<Host, Map<Host,T>> propertyMatrix;

    public HostPropertyMatrix(List<Host> hostList, InputStream matrixConfig) throws IOException {
        propertyMatrix = new ConcurrentHashMap<>();

        BufferedReader reader = new BufferedReader(new InputStreamReader(matrixConfig));
        for (Host hostI: hostList) {
            String[] strValues = reader.readLine().split(" ");
            int j = 0;
            propertyMatrix.put(hostI, new ConcurrentHashMap<>());
            for (Host hostJ: hostList) {
                T property = parseString(strValues[j++]);
                propertyMatrix.get(hostI).put(hostJ, property);
            }
        }
    }

    protected abstract T parseString(String input);

    public T getProperty(Host host1, Host host2) {
        return propertyMatrix.get(host1).get(host2);
    }

    public T changeProperty(Host host1, Host host2, T newValue) {
        propertyMatrix.get(host1).put(host2, newValue);
        return propertyMatrix.get(host2).put(host1, newValue);
    }
}
