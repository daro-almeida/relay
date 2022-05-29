package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

public class RelayConnectionCloseMessage extends RelayMessage {

	public static final IRelaySerializer serializer = new IRelaySerializer<RelayConnectionCloseMessage>() {
		@Override
		public void serialize(RelayConnectionCloseMessage msg, ByteBuf out) {
			out.writeInt(msg.cause.getMessage().getBytes().length);
			out.writeBytes(msg.cause.getMessage().getBytes());
		}

		@Override
		public RelayConnectionCloseMessage deserialize(int seqN, Host from, Host to, ByteBuf in) {
			int size = in.readInt();
			byte[] strBytes = new byte[size];
			in.readBytes(strBytes);
			String message = new String(strBytes);

			return new RelayConnectionCloseMessage(seqN, from, to, new Throwable(message));
		}
	};
	private final Throwable cause;

	public RelayConnectionCloseMessage(Host from, Host to, Throwable cause) {
		super(from, to, Type.CONN_CLOSE);
		this.cause = cause;
	}

	public RelayConnectionCloseMessage(int seqN, Host from, Host to, Throwable cause) {
		super(seqN, from, to, Type.CONN_CLOSE);
		this.cause = cause;
	}
}
