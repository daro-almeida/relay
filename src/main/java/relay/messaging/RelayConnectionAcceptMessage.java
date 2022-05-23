package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class RelayConnectionAcceptMessage extends RelayMessage {

    public RelayConnectionAcceptMessage(Host from, Host to) {
        super(from, to, Type.CONN_ACCEPT);
    }

    public RelayConnectionAcceptMessage(int seqN, Host from, Host to) {
        super(seqN, from, to, Type.CONN_ACCEPT);
    }

    public static final IRelaySerializer serializer = new IRelaySerializer<RelayConnectionAcceptMessage>() {
        @Override
        public void serialize(RelayConnectionAcceptMessage msg, ByteBuf out) throws IOException {
            //nothing to do here
        }

        @Override
        public RelayConnectionAcceptMessage deserialize(int seqN, Host from, Host to, ByteBuf in) throws IOException {
            return new RelayConnectionAcceptMessage(seqN, from, to);
        }
    };
}
