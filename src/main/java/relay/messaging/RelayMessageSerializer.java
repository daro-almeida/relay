package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class RelayMessageSerializer implements ISerializer<RelayMessage> {

    @Override
    public void serialize(RelayMessage relayMessage, ByteBuf out) throws IOException {
        out.writeInt(relayMessage.getType().opCode);
        relayMessage.getType().serializer.serialize(relayMessage, out);
    }

    @Override
    public RelayMessage deserialize(ByteBuf in) throws IOException {
        RelayMessage.Type type = RelayMessage.Type.fromOpcode(in.readInt());
        return type.serializer.deserialize(in);
    }

}
