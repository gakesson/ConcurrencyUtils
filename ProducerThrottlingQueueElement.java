package gakesson.util.concurrent;

/**
 * An interface representing a queue element in a
 * {@link ProducerThrottlingBlockingQueue}.
 * 
 */
public interface ProducerThrottlingQueueElement {
	/**
	 * This type describes the different throttling options for queue elements
	 * when producer capacity is violated.
	 * 
	 */
	public static enum ThrottlingPolicy {
		/**
		 * The queue will reject the element and not insert it into the backing
		 * queue.
		 */
		MUST_THROTTLE,

		/**
		 * The queue attempts to throttle, but in case it fails elements with
		 * this setting will still attempt to be inserted into the backing
		 * queue.
		 */
		ATTEMPT_THROTTLE,
	}

	/**
	 * Returns the object used to identify which entity that produced this
	 * particular {@link ProducerThrottlingQueueElement}. This identifier along
	 * with the allowed producer capacity (retrieved using
	 * {@link #getAllowedProducerCapacity()} is used for blocking mechanisms
	 * when the number of elements from a particular producer reaches the
	 * maximum allowed capacity.
	 * 
	 * @return
	 */
	public Object getProducerIdentifier();

	/**
	 * Returns the maximum allowed capacity of a particular producer.
	 * 
	 * @return
	 */
	public int getAllowedProducerCapacity();

	/**
	 * Returns the {@link ThrottlingPolicy} of this particular element.
	 * 
	 * @return
	 */
	public ThrottlingPolicy getThrottlingPolicy();
}
