import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

public class StringSerializer implements ISerializer<String> {

    @Override
    public void serialize(String str, ByteBuf out) {
        out.writeInt(str.getBytes().length);
        out.writeBytes(str.getBytes());
    }

    @Override
    public String deserialize(ByteBuf in) {
        int size = in.readInt();
        byte[] strBytes = new byte[size];
        in.readBytes(strBytes);
        return new String(strBytes);
    }
}
