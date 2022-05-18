package relay.messaging;

import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.ISerializer;
import relay.Relay;

import java.io.IOException;

public class RelayMessageSerializer implements ISerializer<RelayMessage> {

    private static final Logger logger = LogManager.getLogger(RelayMessageSerializer.class);

    @Override
    public void serialize(RelayMessage relayMessage, ByteBuf out) throws IOException {
        out.writeInt(relayMessage.getType().opCode);
        relayMessage.getType().serializer.serialize(relayMessage, out);
        logger.debug("Serialized message to "+relayMessage.to+" from "+relayMessage.from);
    }

    @Override
    public RelayMessage deserialize(ByteBuf in) throws IOException {
        RelayMessage.Type type = RelayMessage.Type.fromOpcode(in.readInt());
        RelayMessage relayMessage = type.serializer.deserialize(in);
        logger.debug("Deserialized message to "+relayMessage.to+" from "+relayMessage.from);
        return relayMessage;
    }

}
