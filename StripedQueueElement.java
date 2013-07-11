package gakesson.util.concurrent;

/**
 * An interface representing a queue element in a {@link StripedBlockingQueue}.
 * 
 * @author Gustav Akesson - gustav.r.akesson@gmail.com
 * 
 */
public interface StripedQueueElement
{
    /**
     * Returns the priority level for this particular queue element. This enum
     * type has to be the same type as provided when the
     * {@link StripedBlockingQueue} was instantiated.
     * 
     * @return
     */
    public Enum<?> getElementPriority();
}
