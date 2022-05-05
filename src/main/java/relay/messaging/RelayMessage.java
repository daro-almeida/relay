package relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public abstract class RelayMessage {

    public enum Type {
        APP_MSG(0, RelayAppMessage.serializer),
        CONN_OPEN(1, RelayConnectionOpenMessage.serializer),
        CONN_CLOSE(2, RelayConnectionCloseMessage.serializer),
        CONN_ACCEPT(3, RelayConnectionAcceptMessage.serializer),
        CONN_FAIL(4, RelayConnectionFailMessage.serializer),
        PEER_DEAD(5, RelayPeerDisconnectedMessage.serializer);

        public final int opCode;
        public final IRelaySerializer<RelayMessage> serializer;
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

        Type(int opCode, IRelaySerializer<RelayMessage> serializer){
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

    private final Type type;
    protected Host from, to;

    public RelayMessage(Host from, Host to, Type type){
        this.type = type;
        this.from = from;
        this.to = to;
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

    public interface IRelaySerializer<T extends RelayMessage> {
        void serialize(T msg, ByteBuf out) throws IOException;
        T deserialize(ByteBuf in) throws IOException;
    }
}
