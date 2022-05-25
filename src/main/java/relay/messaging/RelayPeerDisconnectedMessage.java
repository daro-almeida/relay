package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class RelayPeerDisconnectedMessage extends RelayMessage {

	public static final IRelaySerializer serializer = new IRelaySerializer<RelayPeerDisconnectedMessage>() {
		@Override
		public void serialize(RelayPeerDisconnectedMessage msg, ByteBuf out) throws IOException {
			out.writeInt(msg.cause.getMessage().getBytes().length);
			out.writeBytes(msg.cause.getMessage().getBytes());
		}

		@Override
		public RelayPeerDisconnectedMessage deserialize(int seqN, Host from, Host to, ByteBuf in) throws IOException {
			int size = in.readInt();
			byte[] strBytes = new byte[size];
			in.readBytes(strBytes);
			String message = new String(strBytes);

			return new RelayPeerDisconnectedMessage(from, to, new Throwable(message));
		}
	};
	private final Throwable cause;

	public RelayPeerDisconnectedMessage(Host from, Host to, Throwable cause) {
		super(from, to, Type.PEER_DEAD);
		this.cause = cause;
	}
}
