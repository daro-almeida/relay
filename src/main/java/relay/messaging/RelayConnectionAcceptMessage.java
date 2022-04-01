package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class RelayConnectionAcceptMessage extends RelayMessage {

    public RelayConnectionAcceptMessage(Host from, Host to) {
        super(from, to, Type.CONN_ACCEPT);
    }

    public static final IRelaySerializer serializer = new IRelaySerializer<RelayConnectionAcceptMessage>() {
        @Override
        public void serialize(RelayConnectionAcceptMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.from, out);
            Host.serializer.serialize(msg.to, out);
        }

        @Override
        public RelayConnectionAcceptMessage deserialize(ByteBuf in) throws IOException {
            Host from = Host.serializer.deserialize(in);
            Host to = Host.serializer.deserialize(in);

            return new RelayConnectionAcceptMessage(from, to);
        }
    };
}
