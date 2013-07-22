package gakesson.util.concurrent;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bounded {@link BlockingQueue} which wraps/decorates any type of {@link Queue} in order to supply the same blocking operations as
 * {@link ArrayBlockingQueue}. This class does not permit {@code null} elements.<br/>
 * <br/>
 * This queue implementation is a classic bounded buffer, in which a fixed-sized array holds elements inserted by producers and extracted by
 * consumers. Once created, the capacity cannot be increased. Attempts to e.g. {@link #put(Object)} an element into a full queue
 * will result in the operation blocking; attempts to e.g. {@link #take()} an element from an empty queue will similarly block.<br/>
 * <br/>
 * This class supports an optional fairness policy for ordering waiting producer and consumer threads. By default, this ordering is not guaranteed.
 * However, a queue constructed with fairness set to true grants threads access in FIFO order. Fairness generally decreases throughput but reduces
 * variability and avoids thread starvation.<br/>
 * <br/>
 * This class and its iterator implement all of the methods of the {@link Collection} and {@link Iterator} interfaces. The Iterator
 * provided in method {@link #iterator()} makes no guarantees except those specified by the wrapped {@link Queue}. Also, method {@code #drainTo} can
 * be used to remove some or all elements of the wrapped queue and place them in another collection.<br/>
 * <br/>
 * This class is thread-safe and does not put any restrictions on the thread-safety of the provided {@link Queue}.<br/>
 * <br/>
 * Note that this implementation relies heavily on {@link Queue#size()}, which means that performance will be affected in case a backing queue is
 * provided of which size method is not constant in time.<br/>
 * <br/>
 * Further, the behavior of this class is undefined if the provided backing queue is modified while instantiation is in progress. The same notion
 * applies for attempting to modify the backing queue outside this class after instantiation has completed.
 * 
 * @author Gustav Akesson - gustav.r.akesson@gmail.com
 * @param <E>
 *            The type of elements held in this {@link BoundedBlockingQueue}
 * 
 */
public class BoundedBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E>
{
    /**
     * The maximum number of allowed elements in this queue.
     */
    private final int myCapacity;

    /**
     * The backing {@link Queue} which this class wraps/decorates.
     */
    private final Queue<E> myBackingQueue;

    /**
     * Lock used for mutual exclusion.
     */
    private final ReentrantLock myLock;

    /**
     * Condition for blocking when empty.
     */
    private final Condition myNotEmpty;

    /**
     * Condition for blocking when full.
     */
    private final Condition myNotFull;

    /**
     * Creates a {@code BoundedBlockingQueue} with default access policy and the provided capacity and backing queue.
     * 
     * @param capacity
     *            The capacity for this queue.
     * @param backingQueue
     *            The backing queue to wrap/decorate.
     * @throws IllegalArgumentException
     *             If {@code capacity} is less than 1, or if the size of the provided queue is greater than the capacity.
     */
    public BoundedBlockingQueue(int capacity, Queue<E> backingQueue)
    {
        this(capacity, backingQueue, false);
    }

    /**
     * Creates a {@code BoundedBlockingQueue} with the specified capacity, backing queue and access policy.
     * 
     * @param capacity
     *            The capacity for this queue.
     * @param backingQueue
     *            The backing queue to wrap/decorate.
     * @param fair
     *            If {@code true} then queue accesses for threads blocked on insertion or removal, are processed in FIFO order; if {@code false} the
     *            access order is unspecified.
     * @throws IllegalArgumentException
     *             If {@code capacity} is less than 1, or if the current size of the provided queue is greater than the capacity.
     */
    public BoundedBlockingQueue(int capacity, Queue<E> backingQueue, boolean fair)
    {
        if (capacity < 1 || backingQueue.size() > capacity)
        {
            throw new IllegalArgumentException();
        }

        myLock = new ReentrantLock(fair);
        myNotEmpty = myLock.newCondition();
        myNotFull = myLock.newCondition();
        myCapacity = capacity;
        myBackingQueue = backingQueue;
    }

    /**
     * Inserts the specified element into this queue if it is possible to do so immediately without exceeding the queue's capacity, returning
     * {@code true} upon success and throwing an {@code IllegalStateException} if this queue is full.
     * 
     * @param element
     *            The element to add.
     * @return {@code true}, as specified by {@link Collection#add}.
     * @throws IllegalStateException
     *             If this queue is full.
     * @throws NullPointerException
     *             If the specified element is null.
     */
    @Override
    public boolean add(E element)
    {
        return super.add(element);
    }

    /**
     * Retrieves and removes the head of this queue, waiting if necessary until an element becomes available.
     * 
     * @return The head of this queue.
     * @throws InterruptedException
     *             If thread is interrupted while waiting for an element to become available.
     */
    @Override
    public E take() throws InterruptedException
    {
        myLock.lockInterruptibly();

        try
        {
            while (myBackingQueue.isEmpty())
            {
                myNotEmpty.await();
            }

            E element = myBackingQueue.poll();
            myNotFull.signal();
            return element;
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Inserts the specified element into this queue, waiting for space to become available if the queue is full.
     * 
     * @param element
     *            The element to add.
     * @throws InterruptedException
     *             If thread is interrupted while waiting for insertion.
     * @throws NullPointerException
     *             If the specified element is null.
     */
    @Override
    public void put(E element) throws InterruptedException
    {
        checkNotNull(element);
        myLock.lockInterruptibly();

        try
        {
            while (myBackingQueue.size() == myCapacity)
            {
                myNotFull.await();
            }

          	myBackingQueue.add(element);
            myNotEmpty.signal();
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Inserts the specified element into this queue if it is possible to do so immediately without exceeding the queue's capacity,
     * returning {@code true} upon success and {@code false} if this queue is full. This method is generally preferable to {@link #add}, which
     * can fail to insert an element only by throwing an exception.
     * 
     * @param element
     *            The element to insert.
     * @throws NullPointerException
     *             If the specified element is null.
     * @return {@code true} upon successful insertion, otherwise {@code false}.
     */
    @Override
    public boolean offer(E element)
    {
        checkNotNull(element);
        boolean inserted = false;
        myLock.lock();

        try
        {
            if (myBackingQueue.size() < myCapacity)
            {
                inserted = myBackingQueue.offer(element);

                if (inserted)
                {
                    myNotEmpty.signal();
                }
            }
        }
        finally
        {
            myLock.unlock();
        }

        return inserted;
    }

    /**
     * Inserts the specified element into this queue, waiting up to the specified wait time for space to become available if the queue is full.
     * 
     * @param element
     *            The element to insert.
     * @param timeout
     *            How long to wait before giving up, in units of {@link TimeUnit}.
     * @param unit
     *            A {@link TimeUnit} determining how to interpret the timeout parameter.
     * @throws InterruptedException
     *             If thread is interrupted while waiting for insertion.
     * @throws NullPointerException
     *             If the specified element is null.
     * @return {@code true} upon successful insertion, otherwise {@code false}.
     */
    @Override
    public boolean offer(E element, long timeout, TimeUnit unit) throws InterruptedException
    {
        checkNotNull(element);
        long nanos = unit.toNanos(timeout);
        myLock.lockInterruptibly();

        try
        {
            for (;;)
            {
                if (myBackingQueue.size() < myCapacity)
                {
                    boolean inserted = myBackingQueue.offer(element);

                    if (inserted)
                    {
                        myNotEmpty.signal();
                        return inserted;
                    }
                }
                
                if (nanos <= 0)
                {
                    return false;
                }

                nanos = myNotFull.awaitNanos(nanos);
            }
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Retrieves and removes the head of this queue, or returns null if this queue is empty.
     * 
     * @return The head of this queue, or null if this queue is empty.
     */
    @Override
    public E poll()
    {
        myLock.lock();

        try
        {
            E element = myBackingQueue.poll();

            if (element != null)
            {
                myNotFull.signal();
            }

            return element;
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Retrieves and removes the head of this queue, waiting up to the specified wait time if necessary for an element to become available.
     * 
     * @param timeout
     *            How long to wait before giving up, in units of {@link TimeUnit}.
     * @param unit
     *            A {@link TimeUnit} determining how to interpret the timeout parameter.
     * @return The head of this queue, or null if the specified waiting time elapses before an element is available
     * @throws InterruptedException
     *             If thread is interrupted while waiting for an element to become available.
     */
    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException
    {
        long nanos = unit.toNanos(timeout);
        myLock.lockInterruptibly();

        try
        {
            for (;;)
            {
                if (!myBackingQueue.isEmpty())
                {
                    E element = myBackingQueue.poll();
                    myNotFull.signal();
                    return element;
                }
                
                if (nanos <= 0)
                {
                    return null;
                }
                
                nanos = myNotEmpty.awaitNanos(nanos);
            }
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Retrieves, but does not remove, the head of this queue, or returns null if this queue is empty.
     * 
     * @return The head of this queue, or null if this queue is empty.
     */
    @Override
    public E peek()
    {
        myLock.lock();

        try
        {
            return myBackingQueue.peek();
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Returns the number of elements currently in this queue.
     * 
     * @return The number of elements in this queue
     */
    @Override
    public int size()
    {
        myLock.lock();

        try
        {
            return myBackingQueue.size();
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Returns the number of additional elements that this queue can ideally (in the absence of memory or resource constraints) accept without
     * blocking. This is always equal to the capacity minus the current {@link #size()} of the backing queue.<br/>
     * <br/>
     * Note that you cannot always tell if an attempt to insert an element will succeed by inspecting {@link #remainingCapacity()} because it
     * may be the case that another thread is about to insert or remove an element.
     * 
     * @return The number of additional elements that this queue can ideally accept without blocking.
     */
    @Override
    public int remainingCapacity()
    {
        myLock.lock();

        try
        {
            return myCapacity - myBackingQueue.size();
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Removes a single instance of the specified element from this queue (if it is present). More formally, removes an element {@code e} such that
     * {@code o.equals(e)}, if this queue contains such an element. Returns {@code true} if this queue contained the specified element (or
     * equivalently, if this queue changed as a result of the call).<br/>
     * <br/>
     * Removal of interior elements in certain queues is an intrinsically slow and disruptive operation, so should be undertaken only in
     * exceptional circumstances, ideally only when the queue is known not to be accessible by other threads.
     * 
     * @param object
     *            Element to be removed from this queue (if present).
     * @return {@code true} if this queue changed as a result of the call.
     */
    @Override
    public boolean remove(Object object)
    {
        myLock.lock();

        try
        {
            boolean removed = myBackingQueue.remove(object);

            if (removed)
            {
                myNotFull.signal();
            }

            return removed;
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Returns {@code true} if this queue contains the specified element. More formally, returns {@code true} if and only if this queue contains at
     * least one element {@code e} such that {@code o.equals(e)}.
     * 
     * @param object
     *            Object to be checked for containment in this queue.
     * @return {@code true} if this queue contains the specified element.
     */
    @Override
    public boolean contains(Object object)
    {
        myLock.lock();

        try
        {
            return myBackingQueue.contains(object);
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Removes all available elements from this queue and adds them to the given collection. This operation may be more efficient than repeatedly
     * polling this queue. A failure encountered while attempting to add elements to the provided collection may result in elements being in neither,
     * either or both collections when the associated exception is thrown. Attempts to drain a queue to itself result in
     * {@link IllegalArgumentException}. Further, the behavior of this operation is undefined if the provided collection is modified while the
     * operation is in progress.
     * 
     * @param collectionToDrainTo
     *            The collection to transfer elements into.
     * @return The number of elements transferred.
     * @throws UnsupportedOperationException
     *             If addition of elements is not supported by the specified collection.
     * @throws ClassCastException
     *             If the class of an element of this queue prevents it from being added to the specified collection.
     * @throws NullPointerException
     *             If the specified collection is null.
     * @throws IllegalArgumentException
     *             If the specified collection is this queue, or some property of an element of this queue prevents it from being added to the
     *             specified collection.
     */
    @Override
    public int drainTo(Collection<? super E> collectionToDrainTo)
    {
        return drainTo(collectionToDrainTo, Integer.MAX_VALUE);
    }

    /**
     * Removes at most the provided number of available elements from this queue and adds them to the provided collection. A failure encountered while
     * attempting to add elements to the provided collection may result in elements being in neither, either or both collections when the associated
     * exception is thrown. Attempts to drain a queue to itself result in {@link IllegalArgumentException}. Further, the behavior of this operation
     * is undefined if the provided collection is modified while the operation is in progress.
     * 
     * @param collectionToDrainTo
     *            The collection to transfer elements into.
     * @param maxElementsToDrain
     *            The maximum number of elements to transfer.
     * @return The number of elements transferred.
     * @throws UnsupportedOperationException
     *             If addition of elements is not supported by the specified collection.
     * @throws ClassCastException
     *             If the class of an element of this queue prevents it from being added to the specified collection.
     * @throws NullPointerException
     *             If the specified collection is null.
     * @throws IllegalArgumentException
     *             If the specified collection is this queue, or some property of an element of this queue prevents it from being added to the
     *             specified collection.
     */
    @Override
    public int drainTo(Collection<? super E> collectionToDrainTo, int maxElementsToDrain)
    {
        checkNotNull(collectionToDrainTo);
        checkNotSame(collectionToDrainTo, this);
        int numberOfDrainedElements = 0;
        myLock.lock();

        try
        {
            E drainedElement;
            while (numberOfDrainedElements < maxElementsToDrain && (drainedElement = myBackingQueue.poll()) != null)
            {
                collectionToDrainTo.add(drainedElement);
                ++numberOfDrainedElements;
            }

            if (numberOfDrainedElements > 0)
            {
                myNotFull.signalAll();
            }
        }
        finally
        {
            myLock.unlock();
        }

        return numberOfDrainedElements;
    }

    /**
     * Atomically removes all of the elements from this queue. The queue will be empty after this call returns.
     * 
     */
    @Override
    public void clear()
    {
        myLock.lock();

        try
        {
            myBackingQueue.clear();
            myNotFull.signalAll();
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Returns an array containing all of the elements in this queue. The returned array elements are in the order specified by the
     * backing queue.<br/>
     * <br/>
     * The returned array will only be as "safe" as the backing queue's {@link Queue#toArray()} implementation.
     * 
     * @return An array containing all of the elements in this queue.
     */
    @Override
    public Object[] toArray()
    {
        myLock.lock();

        try
        {
            return myBackingQueue.toArray();
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Returns an array containing all of the elements in this queue; the runtime type of the returned array is that of the specified array. The
     * returned array elements are in the order specified by the backing queue. If the queue fits in the specified array, it is returned therein. 
     * Otherwise, a new array is allocated with the runtime type of the specified array and the size of this queue. If this queue fits in the 
     * specified array with room to spare (i.e., the array has more elements than this queue), the element in the array immediately following the 
     * end of the queue is set to null.<br/>
     * <br/>
     * Note that {@code toArray(new Object[0])} is identical in function to {@code toArray()}.
     * 
     * @param newArray
     *            The array into which the elements of the queue are to be stored, if it is big enough; otherwise, a new array of the same runtime
     *            type is allocated for this purpose.
     * @return An array containing all of the elements in this queue.
     * @throws ArrayStoreException
     *             If the runtime type of the specified array is not a supertype of the runtime type of every element in this queue.
     * @throws NullPointerException
     *             If the specified array is null.
     */
    @Override
    public <T> T[] toArray(T[] newArray)
    {
        myLock.lock();

        try
        {
            return myBackingQueue.toArray(newArray);
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Returns an iterator over the elements in this queue. The iterator will return its' elements in the order specified by the backing queue's
     * {@link Queue#iterator()} implementation.<br/>
     * <br/>
     * The returned iterator is a "weakly consistent" iterator that will never throw {@link ConcurrentModificationException}, and guarantees to
     * traverse elements as they existed upon construction of the iterator, and may (but is not guaranteed to) reflect any modifications subsequent to
     * construction.
     * 
     * @return An {@link Iterator} over the elements in this queue.
     */
    @Override
    public Iterator<E> iterator()
    {
        myLock.lock();

        try
        {
            return new QueueIterator();
        }
        finally
        {
            myLock.unlock();
        }
    }

    /**
     * Verifies that the provided object is not null, and if it is a {@link NullPointerException} is thrown.
     * 
     * @param object
     *            The object to verify.
     */
    private static void checkNotNull(Object object)
    {
        if (object == null)
        {
            throw new NullPointerException();
        }
    }

    /**
     * Verifies that the two provided references don't refer to the very same object, and if they do an {@link IllegalArgumentException} is thrown.
     * 
     * @param first
     *            The first reference to compare.
     * @param second
     *            The second reference to compare.
     */
    private static void checkNotSame(Object first, Object second)
    {
        if (first == second)
        {
            throw new IllegalArgumentException("Not allowed due to same object");
        }
    }

    /**
     * An {@link Iterator} implementation which takes a snapshot of the backing queue.
     * 
     */
    private final class QueueIterator implements Iterator<E>
    {
        private static final int NO_INDEX = -1;

        private final Object[] myArray;
        private int myCurrentIndex;
        private int myPreviousIndex;

        /**
         * Creates a new {@link QueueIterator} instance.
         * 
         */
        QueueIterator()
        {
            myPreviousIndex = NO_INDEX;
            myArray = myBackingQueue.toArray();
        }

        @Override
        public boolean hasNext()
        {
            return myCurrentIndex < myArray.length;
        }

        @Override
        @SuppressWarnings("unchecked")
        public E next()
        {
            if (myCurrentIndex >= myArray.length)
            {
                throw new NoSuchElementException();
            }

            myPreviousIndex = myCurrentIndex;
            return (E) myArray[myCurrentIndex++];
        }

        @Override
        public void remove()
        {
            if (myPreviousIndex == NO_INDEX)
            {
                throw new IllegalStateException();
            }

            removeElementByIdentity(myArray[myPreviousIndex]);
            myPreviousIndex = NO_INDEX;
        }

        /**
         * Identity-based removal of the provided object. This method will be executed using mutual exclusion.
         * 
         * @param objectToRemove
         */
        private void removeElementByIdentity(Object objectToRemove)
        {
            myLock.lock();

            try
            {
                Iterator<E> iterator = myBackingQueue.iterator();

                while (iterator.hasNext())
                {
                    E element = iterator.next();

                    if (objectToRemove == element)
                    {
                        iterator.remove();
                        myNotFull.signal();
                        break;
                    }
                }
            }
            finally
            {
                myLock.unlock();
            }
        }
    }
}
