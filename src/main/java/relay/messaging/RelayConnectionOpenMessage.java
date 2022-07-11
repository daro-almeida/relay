package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

public class RelayConnectionOpenMessage extends RelayMessage {

	public static final IRelaySerializer serializer = new IRelaySerializer<RelayConnectionOpenMessage>() {
		@Override
		public void serialize(RelayConnectionOpenMessage msg, ByteBuf out) {
			//nothing to do here
		}

		@Override
		public RelayConnectionOpenMessage deserialize(int seqN, Host from, Host to, long sentTime, ByteBuf in) {
			return new RelayConnectionOpenMessage(seqN, from, to, sentTime);
		}
	};

	public RelayConnectionOpenMessage(int seqN, Host from, Host to, long sentTime) {
		super(seqN, from, to, sentTime, Type.CONN_OPEN);
	}
}
