package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class RelayConnectionFailMessage extends RelayMessage {

    private final Throwable cause;

    public RelayConnectionFailMessage(Host from, Host to, Throwable cause) {
        super(from, to, Type.CONN_FAIL);
        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }

    public static final IRelaySerializer serializer = new IRelaySerializer<RelayConnectionFailMessage>() {
        @Override
        public void serialize(RelayConnectionFailMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.cause.getMessage().getBytes().length);
            out.writeBytes(msg.cause.getMessage().getBytes());
        }

        @Override
        public RelayConnectionFailMessage deserialize(int seqN, Host from, Host to, ByteBuf in) throws IOException {
            int size = in.readInt();
            byte[] strBytes = new byte[size];
            in.readBytes(strBytes);
            String message = new String(strBytes);

            return new RelayConnectionFailMessage(from, to, new Throwable(message));
        }
    };
}
