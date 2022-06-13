package relay.util;

import pt.unl.fct.di.novasys.network.data.Host;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class HostPropertyList<T> {

	protected final Map<Host, T> propertyList;

	protected HostPropertyList(List<Host> hostList, InputStream listConfig) throws IOException {
		propertyList = new ConcurrentHashMap<>();

		BufferedReader reader = new BufferedReader(new InputStreamReader(listConfig));
		for (Host host : hostList) {
			String strProperty = reader.readLine();
			propertyList.put(host, parseString(strProperty));
		}
	}

	protected abstract T parseString(String input);

	public final T getProperty(Host host) {
		return propertyList.get(host);
	}

	public final T changeProperty(Host host, T newValue) {
		return propertyList.put(host, newValue);
	}
}
