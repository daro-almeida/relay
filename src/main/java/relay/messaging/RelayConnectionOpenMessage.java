package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class RelayConnectionOpenMessage extends RelayMessage {

    public RelayConnectionOpenMessage(Host from, Host to) {
        super(from, to, Type.CONN_OPEN);
    }

    public static final IRelaySerializer serializer = new IRelaySerializer<RelayConnectionOpenMessage>() {
        @Override
        public void serialize(RelayConnectionOpenMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.from, out);
            Host.serializer.serialize(msg.to, out);
        }

        @Override
        public RelayConnectionOpenMessage deserialize(ByteBuf in) throws IOException {
            Host from = Host.serializer.deserialize(in);
            Host to = Host.serializer.deserialize(in);
            
            return new RelayConnectionOpenMessage(from, to);
        }
    };
}
