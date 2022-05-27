package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class RelayAppMessage extends RelayMessage {
	public static final IRelaySerializer serializer = new IRelaySerializer<RelayAppMessage>() {
		@Override
		public void serialize(RelayAppMessage msg, ByteBuf out) throws IOException {
			out.writeBytes(msg.payload);
		}

		@Override
		public RelayAppMessage deserialize(int seqN, Host from, Host to, ByteBuf in) throws IOException {
			int msgSize = in.getInt(in.readerIndex());
			byte[] content = new byte[msgSize];
			in.skipBytes(4);
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
}
