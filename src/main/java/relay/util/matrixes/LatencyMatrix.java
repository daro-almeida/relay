package relay.util.matrixes;

import pt.unl.fct.di.novasys.network.data.Host;
import relay.util.HostPropertySymmetricMatrix;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class LatencyMatrix extends HostPropertySymmetricMatrix<Float> {

	public LatencyMatrix(List<Host> hostList, InputStream matrixConfig) throws IOException {
		super(hostList, matrixConfig);
	}

	public LatencyMatrix(List<Host> hostList, InputStream matrixConfig, int relayID, int numRelays) throws IOException {
		super(hostList, matrixConfig, relayID, numRelays);
	}

	@Override
	protected Float parseString(String input) {
		return Float.parseFloat(input);
	}
}
