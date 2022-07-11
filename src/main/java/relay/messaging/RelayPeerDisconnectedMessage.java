package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

public class RelayPeerDisconnectedMessage extends RelayMessage {

	public static final IRelaySerializer serializer = new IRelaySerializer<RelayPeerDisconnectedMessage>() {
		@Override
		public void serialize(RelayPeerDisconnectedMessage msg, ByteBuf out) {
			out.writeInt(msg.cause.getMessage().getBytes().length);
			out.writeBytes(msg.cause.getMessage().getBytes());
		}

		@Override
		public RelayPeerDisconnectedMessage deserialize(int seqN, Host from, Host to, long sentTime, ByteBuf in) {
			int size = in.readInt();
			byte[] strBytes = new byte[size];
			in.readBytes(strBytes);
			String message = new String(strBytes);

			return new RelayPeerDisconnectedMessage(from, to, sentTime, new Throwable(message));
		}
	};
	private final Throwable cause;

	public RelayPeerDisconnectedMessage(Host from, Host to, Throwable cause) {
		super(from, to, Type.PEER_DISCONNECTED);
		this.cause = cause;
	}

	public RelayPeerDisconnectedMessage(Host from, Host to, long sentTime, Throwable cause) {
		super(-1, from, to, sentTime, Type.PEER_DISCONNECTED);
		this.cause = cause;
	}
}
