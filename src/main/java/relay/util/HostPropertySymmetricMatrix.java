package relay.util;

import pt.unl.fct.di.novasys.network.data.Host;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class HostPropertySymmetricMatrix<T> {

	private final Map<SymPair<Host>, T> propertyMatrix;

	public HostPropertySymmetricMatrix(List<Host> hostList, InputStream matrixConfig) throws IOException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(matrixConfig));

		propertyMatrix = new ConcurrentHashMap<>(); //reader.lines.count() ?

		for (Host hostI : hostList) {
			String[] strValues = reader.readLine().split(" ");
			int j = 0;
			for (Host hostJ : hostList) {
				T property = parseString(strValues[j++]);
				propertyMatrix.put(new SymPair<>(hostI, hostJ), property);
			}
		}
	}

	protected abstract T parseString(String input);

	public final T getProperty(Host host1, Host host2) {
		return propertyMatrix.get(new SymPair<>(host1, host2));
	}

	public final T changeProperty(Host host1, Host host2, T newValue) {
		return propertyMatrix.put(new SymPair<>(host1, host2), newValue);
	}
}
