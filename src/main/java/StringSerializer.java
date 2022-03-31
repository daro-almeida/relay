import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class StringSerializer implements ISerializer<String> {

    @Override
    public void serialize(String str, ByteBuf out) throws IOException {
        out.writeInt(str.getBytes().length);
        out.writeBytes(str.getBytes());
    }

    @Override
    public String deserialize(ByteBuf in) throws IOException {
        int size = in.readInt();
        byte[] strBytes = new byte[size];
        in.readBytes(strBytes);
        return new String(strBytes);
    }
}
