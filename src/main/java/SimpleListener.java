import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.channel.ChannelEvent;
import pt.unl.fct.di.novasys.channel.ChannelListener;
import pt.unl.fct.di.novasys.network.data.Host;


public class SimpleListener<T> implements ChannelListener<T> {

    private static final Logger logger = LogManager.getLogger(SimpleListener.class);

    private final Host self;

    public SimpleListener(Host self) {
        this.self = self;
    }

    @Override
    public void deliverMessage(T msg, Host from) {
        logger.info(self+": got "+msg+" from "+from);
    }

    @Override
    public void messageSent(T msg, Host to) {
        logger.info(self+": sent "+msg+" int to "+to);
    }

    @Override
    public void messageFailed(T msg, Host to, Throwable cause) {
        logger.info(self+": error on sending '"+msg+"' message to "+to+" : "+cause);
    }

    @Override
    public void deliverEvent(ChannelEvent evt) {
        logger.info(self+": Event: "+evt);
    }
}
