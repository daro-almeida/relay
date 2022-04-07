package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class RelayPeerDeadMessage extends RelayMessage {

    private final Throwable cause;

    public RelayPeerDeadMessage(Host from, Host to, Throwable cause) {
        super(from, to, Type.PEER_DEAD);
        this.cause = cause;
    }

    public static final IRelaySerializer serializer = new IRelaySerializer<RelayPeerDeadMessage>() {
        @Override
        public void serialize(RelayPeerDeadMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.from, out);
            Host.serializer.serialize(msg.to, out);

            out.writeInt(msg.cause.getMessage().getBytes().length);
            out.writeBytes(msg.cause.getMessage().getBytes());
        }

        @Override
        public RelayPeerDeadMessage deserialize(ByteBuf in) throws IOException {
            Host from = Host.serializer.deserialize(in);
            Host to = Host.serializer.deserialize(in);

            int size = in.readInt();
            byte[] strBytes = new byte[size];
            in.readBytes(strBytes);
            String message = new String(strBytes);

            return new RelayPeerDeadMessage(from, to, new Throwable(message));
        }
    };
}
