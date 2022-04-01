package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class RelayConnectionCloseMessage extends RelayMessage {

    private final Throwable cause;

    public RelayConnectionCloseMessage(Host from, Host to, Throwable cause) {
        super(from, to, Type.CONN_CLOSE);
        this.cause = cause;
    }

    public static final IRelaySerializer serializer = new IRelaySerializer<RelayConnectionCloseMessage>() {
        @Override
        public void serialize(RelayConnectionCloseMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.from, out);
            Host.serializer.serialize(msg.to, out);

            out.writeInt(msg.cause.getMessage().getBytes().length);
            out.writeBytes(msg.cause.getMessage().getBytes());
        }

        @Override
        public RelayConnectionCloseMessage deserialize(ByteBuf in) throws IOException {
            Host from = Host.serializer.deserialize(in);
            Host to = Host.serializer.deserialize(in);

            int size = in.readInt();
            byte[] strBytes = new byte[size];
            in.readBytes(strBytes);
            String message = new String(strBytes);

            return new RelayConnectionCloseMessage(from, to, new Throwable(message));
        }
    };
}
