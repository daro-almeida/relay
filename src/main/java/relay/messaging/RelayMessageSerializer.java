package relay.messaging;

import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import relay.Relay;

import java.io.IOException;

public class RelayMessageSerializer implements ISerializer<RelayMessage> {

    private static final Logger logger = LogManager.getLogger(RelayMessageSerializer.class);

    @Override
    public void serialize(RelayMessage relayMessage, ByteBuf out) throws IOException {
        out.writeInt(relayMessage.getType().opCode);
        out.writeInt(relayMessage.seqN);
        Host.serializer.serialize(relayMessage.from, out);
        Host.serializer.serialize(relayMessage.to, out);
        relayMessage.getType().serializer.serialize(relayMessage, out);
        logger.debug("Serialized message "+relayMessage.seqN+" to "+relayMessage.to+" from "+relayMessage.from);
    }

    @Override
    public RelayMessage deserialize(ByteBuf in) throws IOException {
        RelayMessage.Type type = RelayMessage.Type.fromOpcode(in.readInt());
        int seqN = in.readInt();
        Host from = Host.serializer.deserialize(in);
        Host to = Host.serializer.deserialize(in);
        RelayMessage relayMessage = type.serializer.deserialize(seqN, from, to, in);
        logger.debug("Deserialized message "+relayMessage.seqN+" to "+relayMessage.to+" from "+relayMessage.from);
        return relayMessage;
    }

}
