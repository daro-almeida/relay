package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

public abstract class RelayMessage {

	protected final Host from;
	protected final Host to;
	protected final int seqN;
	private final Type type;

	protected RelayMessage(Host from, Host to, Type type) {
		this.seqN = -1;
		this.type = type;
		this.from = from;
		this.to = to;
	}

	protected RelayMessage(int seqN, Host from, Host to, Type type) {
		this.seqN = seqN;
		this.type = type;
		this.from = from;
		this.to = to;
	}

	public int getSeqN() {
		return seqN;
	}

	public Type getType() {
		return type;
	}

	public Host getFrom() {
		return from;
	}

	public Host getTo() {
		return to;
	}

	@Override
	public String toString() {
		return "RelayMessage{" +
				"from=" + from +
				", to=" + to +
				", seqN=" + seqN +
				", type=" + type +
				'}';
	}

	public enum Type {
		APP_MSG(0, RelayAppMessage.serializer),
		CONN_OPEN(1, RelayConnectionOpenMessage.serializer),
		CONN_CLOSE(2, RelayConnectionCloseMessage.serializer),
		CONN_ACCEPT(3, RelayConnectionAcceptMessage.serializer),
		CONN_FAIL(4, RelayConnectionFailMessage.serializer),
		PEER_DEAD(5, RelayPeerDisconnectedMessage.serializer);

		private static final Type[] opcodeIdx;

		static {
			int maxOpcode = -1;
			for (Type type : Type.values())
				maxOpcode = Math.max(maxOpcode, type.opCode);
			opcodeIdx = new Type[maxOpcode + 1];
			for (Type type : Type.values()) {
				if (opcodeIdx[type.opCode] != null)
					throw new IllegalStateException("Duplicate opcode");
				opcodeIdx[type.opCode] = type;
			}
		}

		public final int opCode;
		public final IRelaySerializer<RelayMessage> serializer;

		Type(int opCode, IRelaySerializer<RelayMessage> serializer) {
			this.opCode = opCode;
			this.serializer = serializer;
		}

		public static Type fromOpcode(int opcode) {
			if (opcode >= opcodeIdx.length || opcode < 0)
				throw new AssertionError(String.format("Unknown opcode %d", opcode));
			Type t = opcodeIdx[opcode];
			if (t == null)
				throw new AssertionError(String.format("Unknown opcode %d", opcode));
			return t;
		}
	}

	public interface IRelaySerializer<T extends RelayMessage> {
		void serialize(T msg, ByteBuf out);

		T deserialize(int seqN, Host from, Host to, ByteBuf in);
	}
}
