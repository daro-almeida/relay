package relay.messaging;

import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class RelayMessageSerializer implements ISerializer<RelayMessage> {

	private static final Logger logger = LogManager.getLogger(RelayMessageSerializer.class);

	@Override
	public void serialize(RelayMessage relayMessage, ByteBuf out) throws IOException {
		out.writeInt(relayMessage.getType().opCode);
		out.writeInt(relayMessage.getSeqN());
		Host.serializer.serialize(relayMessage.getFrom(), out);
		Host.serializer.serialize(relayMessage.getTo(), out);
		out.writeLong(relayMessage.getSentTime());
		relayMessage.getType().serializer.serialize(relayMessage, out);
		logger.trace("Serialized {} message {} to {} from {}", relayMessage.getType().name(), relayMessage.getSeqN(), relayMessage.getTo(), relayMessage.getFrom());
	}

	@Override
	public RelayMessage deserialize(ByteBuf in) throws IOException {
		RelayMessage.Type type = RelayMessage.Type.fromOpcode(in.readInt());
		int seqN = in.readInt();
		Host from = Host.serializer.deserialize(in);
		Host to = Host.serializer.deserialize(in);
		long sentTime = in.readLong();
		RelayMessage relayMessage = type.serializer.deserialize(seqN, from, to, sentTime, in);
		logger.trace("Deserialized {} message {} to {} from {}", relayMessage.getType().name(), relayMessage.getSeqN(), relayMessage.getTo(), relayMessage.getFrom());
		return relayMessage;
	}

}
