package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

public class RelayConnectionFailMessage extends RelayMessage {

	public static final IRelaySerializer serializer = new IRelaySerializer<RelayConnectionFailMessage>() {
		@Override
		public void serialize(RelayConnectionFailMessage msg, ByteBuf out) {
			out.writeInt(msg.cause.getMessage().getBytes().length);
			out.writeBytes(msg.cause.getMessage().getBytes());
		}

		@Override
		public RelayConnectionFailMessage deserialize(int seqN, Host from, Host to, long sentTime, ByteBuf in) {
			int size = in.readInt();
			byte[] strBytes = new byte[size];
			in.readBytes(strBytes);
			String message = new String(strBytes);

			return new RelayConnectionFailMessage(from, to, sentTime, new Throwable(message));
		}
	};
	private final Throwable cause;

	public RelayConnectionFailMessage(Host from, Host to, Throwable cause) {
		super(from, to, Type.CONN_FAIL);
		this.cause = cause;
	}

	public RelayConnectionFailMessage(Host from, Host to, long sentTime, Throwable cause) {
		super(-1, from, to, sentTime, Type.CONN_FAIL);
		this.cause = cause;
	}
}
