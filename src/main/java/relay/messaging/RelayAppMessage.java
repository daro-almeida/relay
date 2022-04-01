package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class RelayAppMessage extends RelayMessage {
    private final byte[] payload;

    public RelayAppMessage(Host from, Host to, byte[] payload) {
        super(from,to,Type.APP_MSG);

        this.payload = payload;
    }

    public static final IRelaySerializer serializer = new IRelaySerializer<RelayAppMessage>() {
        @Override
        public void serialize(RelayAppMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.from, out);
            Host.serializer.serialize(msg.to, out);

            out.writeBytes(msg.payload);
        }

        @Override
        public RelayAppMessage deserialize(ByteBuf in) throws IOException {
            Host from = Host.serializer.deserialize(in);
            Host to = Host.serializer.deserialize(in);

            int msgSize = in.getInt(in.readerIndex());
            byte[] payload = new byte[msgSize];
            in.skipBytes(4);
            in.readBytes(payload);

            return new RelayAppMessage(from, to, payload);
        }
    };
}
