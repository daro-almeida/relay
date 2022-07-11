package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

public class RelayConnectionAcceptMessage extends RelayMessage {

	public static final IRelaySerializer serializer = new IRelaySerializer<RelayConnectionAcceptMessage>() {
		@Override
		public void serialize(RelayConnectionAcceptMessage msg, ByteBuf out) {
			//nothing to do here
		}

		@Override
		public RelayConnectionAcceptMessage deserialize(int seqN, Host from, Host to, long sentTime, ByteBuf in) {
			return new RelayConnectionAcceptMessage(seqN, from, to, sentTime);
		}
	};

	public RelayConnectionAcceptMessage(int seqN, Host from, Host to, long sentTime) {
		super(seqN, from, to, sentTime, Type.CONN_ACCEPT);
	}
}
