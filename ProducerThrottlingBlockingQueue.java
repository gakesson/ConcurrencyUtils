package gakesson.util.concurrent;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import gakesson.util.concurrent.ProducerThrottlingQueueElement.ThrottlingPolicy;

/**
 * A {@link BlockingQueue} which decorates any type of blocking queue to be able
 * to throttle producers from queueing an unproportional number of elements at
 * the same time.
 * 
 * This queue implementation makes it possible to define a producer-capacity
 * limit based on a producer identifier (retrieved using
 * {@link ProducerThrottlingQueueElement#getProducerIdentifier()}. This is
 * primarily useful when the {@link ProducerThrottlingBlockingQueue} is used to
 * queue elements from several different producers. In order to throttle
 * particular producers from queueing an unproportional amount of elements at
 * the same time, the
 * {@link ProducerThrottlingQueueElement#getAllowedProducerCapacity()} restricts
 * insertions from those particular producers which exceed that defined limit.
 * The behavior when exceeding the capacity is defined by the
 * {@link BlockingQueue} contract. Additionally, if inserting elements from one
 * particular producer blocks due to capacity violation it will not affect the
 * insertion of elements from other producers (unless, of course, they don't
 * violate their defined capacity constraint or that the capacity constraint of
 * the provided {@link BlockingQueue} is violated).
 * 
 * This class supports an optional fairness policy for ordering producer threads
 * which contend for insertions. By default, this ordering is not guaranteed.
 * However, a queue constructed with fairness set to {@code true} grants threads
 * access in FIFO order. Fairness generally decreases throughput but reduces
 * variability and avoids thread starvation.
 * 
 * Taking the this class' per-producer throttling mechanism aside, the behavior
 * of this queue is completely dictated by the provided backing
 * {@link BlockingQueue} implementation. Additionally, it is undefined behavior
 * if the provided backing queue is modified outside of the this class.
 * 
 * Note that {@link ProducerThrottlingBlockingQueue} operations will fail with a
 * {@link ClassCastException} in case its' elements are not of type
 * {@link ProducerThrottlingQueueElement}.
 * 
 * @author Gustav Akesson - gustav.r.akesson@gmail.com
 * 
 * @param <E>
 *            The type of elements held in this collection (must be of type
 *            {@link ProducerThrottlingQueueElement}).
 */
public class ProducerThrottlingBlockingQueue<E> extends AbstractQueue<E>
		implements BlockingQueue<E> {
	/*
	 * The decorated queue.
	 */
	private final BlockingQueue<E> myBackingQueue;

	/*
	 * The access policy for throttled producers.
	 */
	private final boolean myAccessPolicy;

	/*
	 * A data structure used for per-producer capacity restrictions (which is
	 * identified by a producer identifier).
	 */
	private volatile HashMap<Object, AdaptiveSemaphore> myRemainingProducerCapacities = new HashMap<Object, AdaptiveSemaphore>();
	
	/**
	 * Creates a {@link ProducerThrottlingBlockingQueue} with the provided
	 * backing queue and the default access policy.
	 * 
	 * @param backingQueue
	 *            The queue to decorate with a throttling mechanism.
	 */
	public ProducerThrottlingBlockingQueue(BlockingQueue<E> backingQueue) {
		this(backingQueue, false);
	}

	/**
	 * Creates a {@link ProducerThrottlingBlockingQueue} with the provided
	 * backing queue and the specified access policy.
	 * 
	 * @param backingQueue
	 *            The queue to decorate with a throttling mechanism.
	 * @param fair
	 *            If {@code true} then queue accesses for producer threads
	 *            blocked on insertion are processed in FIFO order; if
	 *            {@code false} the access order is unspecified.
	 */
	public ProducerThrottlingBlockingQueue(BlockingQueue<E> backingQueue,
			boolean fair) {
		myBackingQueue = backingQueue;
		myAccessPolicy = fair;
	}

	/**
	 * Inserts the specified element, waiting for space to become available if
	 * the queue is full. This method might block either due to a producer
	 * violating its' capacity, or capacity violation of the backing queue.
	 * 
	 * @param e
	 *            The element to insert.
	 * @throws InterruptedException
	 *             If interrupted while waiting.
	 * @throws ClassCastException
	 *             In case the provided element is not of type
	 *             {@link ProducerThrottlingBlockingQueue}.
	 */
	@Override
	public void put(E e) throws InterruptedException {
		ProducerThrottlingQueueElement element = getProducerThrottlingQueueElement(e);
		AdaptiveSemaphore remainingProducerCapacity = getRemainingProducerCapacity(element);
		boolean acquired = false;
		boolean inserted = false;

		try {
			if (element.getThrottlingPolicy() == ThrottlingPolicy.ATTEMPT_THROTTLE) {
				acquired = remainingProducerCapacity.tryAcquire();

				if (!acquired) {
					remainingProducerCapacity.decrementPermit();
					acquired = true;
				}
			} else {
				remainingProducerCapacity.acquire();
				acquired = true;
			}

			myBackingQueue.put(e);
			inserted = true;
		} finally {
			if (acquired && !inserted) {
				releaseAcquiredProducerPermitFor(e);
			}
		}
	}

	/**
	 * Inserts the specified element if it is possible to do so immediately
	 * without exceeding the producer's capacity or the backing queue's
	 * capacity.
	 * 
	 * @param e
	 *            The element to insert.
	 * @return {@code true} if the element was inserted, otherwise {@code false}
	 *         .
	 * @throws ClassCastException
	 *             In case the provided element is not of type
	 *             {@link ProducerThrottlingBlockingQueue}.
	 */
	@Override
	public boolean offer(E e) {
		ProducerThrottlingQueueElement element = getProducerThrottlingQueueElement(e);
		AdaptiveSemaphore remainingProducerCapacity = getRemainingProducerCapacity(element);
		boolean acquired = remainingProducerCapacity.tryAcquire();
		boolean inserted = false;

		try {
			if (!acquired
					&& element.getThrottlingPolicy() == ThrottlingPolicy.ATTEMPT_THROTTLE) {
				remainingProducerCapacity.decrementPermit();
				acquired = true;
			}

			if (acquired) {
				inserted = myBackingQueue.offer(e);
			}
		} finally {
			if (acquired && !inserted) {
				releaseAcquiredProducerPermitFor(e);
			}
		}

		return inserted;
	}

	/**
	 * Inserts the specified element, waiting up to the specified wait time for
	 * space to become available if this producer has reached its' limit or the
	 * backing queue is full.
	 * 
	 * @param e
	 *            The element to insert.
	 * @param timeout
	 *            How long to wait before giving up, in units of the provided
	 *            {@link TimeUnit} parameter.
	 * @param unit
	 *            A {@link TimeUnit} determining how to interpret the timeout
	 *            parameter
	 * @return {@code true} if the element was inserted, otherwise {@code false}
	 *         .
	 * @throws InterruptedException
	 *             If interrupted while waiting.
	 * @throws ClassCastException
	 *             In case the provided element is not of type
	 *             {@link ProducerThrottlingBlockingQueue}.
	 */
	@Override
	public boolean offer(E e, long timeout, TimeUnit unit)
			throws InterruptedException {
		ProducerThrottlingQueueElement element = getProducerThrottlingQueueElement(e);
		AdaptiveSemaphore remainingProducerCapacity = getRemainingProducerCapacity(element);
		long timeBeforeAcquiringPermit = System.nanoTime();
		boolean acquired = remainingProducerCapacity.tryAcquire(timeout, unit);
		boolean inserted = false;

		try {
			if (!acquired
					&& element.getThrottlingPolicy() == ThrottlingPolicy.ATTEMPT_THROTTLE) {
				remainingProducerCapacity.decrementPermit();
				acquired = true;
			}

			if (acquired) {
				long elapsedTimeInNanosForAcquiringPermit = System.nanoTime()
						- timeBeforeAcquiringPermit;
				timeout = unit.toNanos(timeout)
						- elapsedTimeInNanosForAcquiringPermit;
				inserted = myBackingQueue.offer(e, timeout,
						TimeUnit.NANOSECONDS);
			}
		} finally {
			if (acquired && !inserted) {
				releaseAcquiredProducerPermitFor(e);
			}
		}

		return inserted;
	}

	/**
	 * Retrieves and removes the head of the backing queue, waiting if necessary
	 * until an element becomes available.
	 * 
	 * @return The head of the backing queue.
	 * @throws InterruptedException
	 *             If interrupted while waiting.
	 */
	@Override
	public E take() throws InterruptedException {
		E e = myBackingQueue.take();
		releaseAcquiredProducerPermitFor(e);
		return e;
	}

	/**
	 * Retrieves and removes the head of the backing queue, or returns
	 * {@code null} if the queue is empty.
	 * 
	 * @return The head of the backing queue, or {@code null} if the queue is
	 *         empty.
	 */
	@Override
	public E poll() {
		E e = myBackingQueue.poll();

		if (e != null) {
			releaseAcquiredProducerPermitFor(e);
		}

		return e;
	}

	/**
	 * Retrieves and removes the head of the backing queue, waiting up to the
	 * specified wait time if necessary for an element to become available.
	 * 
	 * @param timeout
	 *            How long to wait before giving up, in units of the provided
	 *            {@link TimeUnit} parameter.
	 * @param unit
	 *            A {@link TimeUnit} determining how to interpret the timeout
	 *            parameter.
	 * @return The head of the backing queue, or {@code null} if the specified
	 *         waiting time elapses before an element is available.
	 * @throws InterruptedException
	 *             If interrupted while waiting.
	 */
	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		E e = myBackingQueue.poll(timeout, unit);

		if (e != null) {
			releaseAcquiredProducerPermitFor(e);
		}

		return e;
	}

	/**
	 * Retrieves, but does not remove, the head of the backing queue, or returns
	 * null if the queue is empty.
	 * 
	 * @return The head of the backing queue, or {@code null} if the queue is
	 *         empty.
	 */
	@Override
	public E peek() {
		return myBackingQueue.peek();
	}

	/**
	 * Removes all available elements from the backing queue and adds them to
	 * the given collection. This operation may be more efficient than
	 * repeatedly polling this queue.
	 * 
	 * A failure encountered while attempting to add elements to the provided
	 * collection may result in elements being in neither, either or both
	 * collections when the associated exception is thrown. Due to the internal
	 * mechanics of the {@link ProducerThrottlingBlockingQueue} it is highly
	 * recommended that when encountering a failure while executing this method,
	 * a new instance should replace this particular queue instance. It might
	 * not be possible to fully recover from such a failure.
	 * 
	 * Attempts to drain the backing queue to itself result in
	 * {@link IllegalArgumentException}. Further, the behavior of this operation
	 * is undefined if the specified collection is modified while the operation
	 * is in progress.
	 * 
	 * @param collectionToDrainTo
	 *            The collection to transfer elements into
	 * @return The number of elements transferred.
	 * @throws NullPointerException
	 *             If the provided collection is null.
	 * @throws IllegalArgumentException
	 *             If the provided collection is the same as this queue or the
	 *             backing queue.
	 */
	@Override
	public int drainTo(Collection<? super E> c) {
		return drainTo(c, Integer.MAX_VALUE);
	}

	/**
	 * Removes at most the provided number of elements from the backing queue
	 * and adds them to the given collection. This operation may be more
	 * efficient than repeatedly polling this queue.
	 * 
	 * A failure encountered while attempting to add elements to the provided
	 * collection may result in elements being in neither, either or both
	 * collections when the associated exception is thrown. Due to the internal
	 * mechanics of the {@link ProducerThrottlingBlockingQueue} it is highly
	 * recommended that when encountering a failure while executing this method,
	 * a new instance should replace this particular queue instance. It might
	 * not be possible to fully recover from such a failure.
	 * 
	 * Attempts to drain a queue to itself result in
	 * {@link IllegalArgumentException}. Further, the behavior of this operation
	 * is undefined if the specified collection is modified while the operation
	 * is in progress.
	 * 
	 * @param collectionToDrainTo
	 *            The collection to transfer elements into.
	 * @param maxElementsToDrain
	 *            The maximum number of elements to transfer.
	 * @return The number of elements transferred.
	 * @throws NullPointerException
	 *             If the provided collection is null.
	 * @throws IllegalArgumentException
	 *             If the provided collection is the same as this queue or the
	 *             backing queue.
	 */
	@Override
	public int drainTo(Collection<? super E> c, int maxElements) {
		checkNotSame(this, c);
		int numberOfDrainedElements = myBackingQueue.drainTo(c, maxElements);
		Iterator<? super E> iterator = c.iterator();

		while (iterator.hasNext()) {
			releaseAcquiredProducerPermitFor(iterator.next());
		}

		return numberOfDrainedElements;
	}

	/**
	 * Removes a single instance of the specified element from the backing
	 * queue, if it is present. More formally, removes an element {@code e} such
	 * that {@code o.equals(e)}, if that queue contains one or more such
	 * elements. Returns {@code true} if the queue contained the specified
	 * element (or equivalently, if the backing queue changed as a result of the
	 * call).
	 * 
	 * @param o
	 *            The instance to remove.
	 * @return {@code true} if an element was removed as a result of this call.
	 * @throws ClassCastException
	 *             In case the provided element is not of type
	 *             {@link ProducerThrottlingQueueElement}.
	 */
	@Override
	public boolean remove(Object o) {
		boolean removed = myBackingQueue.remove(o);

		if (removed) {
			releaseAcquiredProducerPermitFor(o);
		}

		return removed;
	}

	/**
	 * Returns an array containing all of the elements in the backing queue, in
	 * order as defined by that queue implementation.
	 * 
	 * @return An array containing all of the elements in the backing queue.
	 */
	@Override
	public Object[] toArray() {
		return myBackingQueue.toArray();
	}

	/**
	 * Returns an array containing all of the elements in the backing queue, in
	 * order as defined by that queue implementation. The runtime type of the
	 * returned array is that of the provided array.
	 * 
	 * @return An array containing all of the elements in the backing queue.
	 */
	@Override
	public <T> T[] toArray(T[] a) {
		return myBackingQueue.toArray(a);
	}

	/**
	 * Returns an iterator over the elements in the backing queue in order as
	 * defined by that queue implementation.
	 * 
	 * @return An {@link Iterator} over the elements in the backing queue.
	 */
	@Override
	public Iterator<E> iterator() {
		return new QueueIterator();
	}

	/**
	 * Returns {@code true} if the backing queue contains the specified element.
	 * More formally, returns {@code true} if and only if the backing queue
	 * contains at least one element {@code e} such that {@code o.equals(e)}.
	 * 
	 * @param o
	 *            The object to be checked for containment in the backing queue.
	 * @return {@code true} if the backing queue contains the specified element.
	 */
	@Override
	public boolean contains(Object o) {
		return myBackingQueue.contains(o);
	}

	/**
	 * Returns the number of additional elements that the backing queue can
	 * ideally (in the absence of memory constraints and per-producer limits)
	 * accept without blocking.
	 * 
	 * @return The remaining capacity.
	 */
	@Override
	public int remainingCapacity() {
		return myBackingQueue.remainingCapacity();
	}

	/**
	 * Returns the current number of elements in the backing queue.
	 * 
	 * @return The number of elements in the backing queue.
	 */
	@Override
	public int size() {
		return myBackingQueue.size();
	}

	/**
	 * Casts the provided object into a {@link ProducerThrottlingQueueElement},
	 * or throws a {@link ClassCastException} if not possible.
	 * 
	 * @param o
	 * @return
	 */
	private static ProducerThrottlingQueueElement getProducerThrottlingQueueElement(
			Object e) {
		return (ProducerThrottlingQueueElement) e;
	}

	/**
	 * Releases an acquired permit for a particular producer (which is
	 * identified using the provided object).
	 * 
	 * @param e
	 */
	private void releaseAcquiredProducerPermitFor(Object e) {
		AdaptiveSemaphore remainingProducerCapacity = getRemainingProducerCapacity(e);
		remainingProducerCapacity.release();
	}

	/**
	 * Returns the remaining capacity of a particular producer (which is
	 * identified using the provided object).
	 * 
	 * @param e
	 * @return
	 */
	private AdaptiveSemaphore getRemainingProducerCapacity(Object e) {
		ProducerThrottlingQueueElement element = getProducerThrottlingQueueElement(e);
		Object producerIdentifier = element.getProducerIdentifier();
		AdaptiveSemaphore remainingProducerCapacity = myRemainingProducerCapacities
				.get(producerIdentifier);

		if (remainingProducerCapacity == null) {
			synchronized (this) {
				remainingProducerCapacity = myRemainingProducerCapacities
						.get(producerIdentifier);

				if (remainingProducerCapacity == null) {
					remainingProducerCapacity = new AdaptiveSemaphore(
							element.getAllowedProducerCapacity(),
							myAccessPolicy);
					HashMap<Object, AdaptiveSemaphore> remainingProducerCapacities = new HashMap<Object, AdaptiveSemaphore>(
							myRemainingProducerCapacities);
					remainingProducerCapacities.put(producerIdentifier,
							remainingProducerCapacity);
					myRemainingProducerCapacities = remainingProducerCapacities;
				}
			}
		}

		return remainingProducerCapacity;
	}

	/**
	 * Verifies that the two references don't refer to the very same object, and
	 * if they do an {@link IllegalArgumentException} is thrown.
	 * 
	 * @param object
	 */
	private static void checkNotSame(Object first, Object second) {
		if (first == second) {
			throw new IllegalArgumentException("Not allowed due to same object");
		}
	}

	/**
	 * An {@link Iterator} implementation which takes a snapshot of the backing
	 * queue.
	 * 
	 */
	private class QueueIterator implements Iterator<E> {
		private final Iterator<E> myBackingIterator;
		private E myCurrentElement;

		/**
		 * Creates a new {@link QueueIterator} instance.
		 * 
		 */
		QueueIterator() {
			myBackingIterator = myBackingQueue.iterator();
		}

		@Override
		public boolean hasNext() {
			return myBackingIterator.hasNext();
		}

		@Override
		public E next() {
			myCurrentElement = myBackingIterator.next();
			return myCurrentElement;
		}

		@Override
		public void remove() {
			myBackingIterator.remove();
			releaseAcquiredProducerPermitFor(myCurrentElement);
		}
	}
}
