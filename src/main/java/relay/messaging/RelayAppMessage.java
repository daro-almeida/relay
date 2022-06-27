package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

public class RelayAppMessage extends RelayMessage {
	public static final IRelaySerializer serializer = new IRelaySerializer<RelayAppMessage>() {
		@Override
		public void serialize(RelayAppMessage msg, ByteBuf out) {
			out.writeInt(msg.payload.length);
			out.writeBytes(msg.payload);
		}

		@Override
		public RelayAppMessage deserialize(int seqN, Host from, Host to, ByteBuf in) {
			int msgSize = in.readInt();
			byte[] content = new byte[msgSize];
			in.readBytes(content);

			return new RelayAppMessage(seqN, from, to, content);
		}
	};
	private final byte[] payload;

	public RelayAppMessage(Host from, Host to, byte[] payload) {
		super(from, to, Type.APP_MSG);
		this.payload = payload;
	}

	public RelayAppMessage(int seqN, Host from, Host to, byte[] payload) {
		super(seqN, from, to, Type.APP_MSG);
		this.payload = payload;
	}

	public byte[] getPayload() {
		return payload;
	}
}
