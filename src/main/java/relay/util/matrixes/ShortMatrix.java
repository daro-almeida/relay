package relay.util.matrixes;

import pt.unl.fct.di.novasys.network.data.Host;
import relay.util.HostPropertyMatrix;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ShortMatrix extends HostPropertyMatrix<Short> {

    public ShortMatrix(List<Host> hostList, InputStream matrixConfig) throws IOException {
        super(hostList, matrixConfig);
    }

    @Override
    protected Short parseString(String input) {
        return Short.parseShort(input);
    }
}
