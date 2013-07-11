package gakesson.util.concurrent;

import java.lang.reflect.Array;
import java.util.AbstractQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link BlockingQueue} which is backed by one or multiple queues or stripes
 * - one for each user-defined priority level.
 * 
 * This queue implementation makes it possible to provide user-defined priority
 * levels with an assigned weight on each level. These weights will come into
 * play when extracting elements from the queue via e.g. {@link #take()}. For
 * instance, if a queue has defined the levels {HIGH=60%, MEDIUM=30% and
 * LOW=10%} (referred to as three different stripes), and there exists elements
 * in all three stripes, the {@link StripedBlockingQueue} will first extract
 * six(6) elements, then three(3) elements and finally one(1) element
 * (regardless of their insertion order). This procedure will then be repeated.
 * This type of functionality allows a priority-based mechanism without the
 * potential starvation of particular elements (which could happen using a
 * {@link PriorityBlockingQueue}. In case it does not exist elements in all
 * defined stripes, it is not possible to strictly follow the set weights, but
 * all defined priority levels will at least get the slice of their
 * corresponding weight. Additionally, if there exists elements in only one of
 * the stripes, those elements will be extracted in regular FIFO-fashion.
 * 
 * Threads attempting to extract elements from this queue will never block in
 * case the queue has elements to extract (regardless of which priority level
 * they belong to).
 * 
 * This class can be either bounded or unbounded. If specifying a maximum
 * capacity limit then that limit will take into account the combined size of
 * all the queue's stripes. If the limit is reached it could for instance mean
 * that only one stripe has elements, or the elements are equally divided
 * between the stripes. Hence, the bounding limit should primarily be used as a
 * way to prevent excessive queue expansion.
 * 
 * This class supports an optional fairness policy for ordering waiting producer
 * and consumer threads. By default, this ordering is not guaranteed. However, a
 * queue constructed with fairness set to {@code true} grants threads access in
 * FIFO order. Fairness generally decreases throughput but reduces variability
 * and avoids thread starvation.
 * 
 * Memory consistency effects: As with other concurrent collections, actions in
 * a thread prior to placing an object into a {@link StripedBlockingQueue}
 * happen-before actions subsequent to the access or removal of that element
 * from the {@link StripedBlockingQueue} in another thread.
 * 
 * This class and its iterator implement all of the optional methods of the
 * {@link Collection} and {@link Iterator} interfaces.
 * 
 * Note that {@link StripedBlockingQueue} operations will fail with a
 * {@link ClassCastException} in case its' elements are not of type
 * {@link StripedQueueElement}.
 * 
 * @author Gustav Akesson - gustav.r.akesson@gmail.com
 * 
 * @param <E>
 *            The type of elements held in this collection (must be of type
 *            {@link StripedQueueElement}).
 */
public class StripedBlockingQueue<E> extends AbstractQueue<E> implements
        BlockingQueue<E>
{
    /*
     * The fixed length of the priority sequence to circulate when consuming
     * elements.
     */
    private static final int PRIORITY_SEQUENCE_LENGTH = 10;

    /*
     * The access policy of this queue.
     */
    private final boolean myAccessPolicy;

    /*
     * The lock used to access this queue.
     */
    private final ReentrantLock myAccessLock;

    /*
     * A signal for consumers that the queue is not empty.
     */
    private final Condition myNotEmpty;

    /*
     * A signal for producers that the queue is not full.
     */
    private final Condition myNotFull;

    /*
     * The total number of allowed elements in this queue.
     */
    private final int myTotalCapacity;

    /*
     * The total number of elements currently in this queue.
     */
    private int myCurrentSize = 0;

    /*
     * The queues identified by the defined priority levels.
     */
    private final EnumMap<?, ArrayDeque<E>> myQueues;

    /*
     * The priority sequence, i.e. the sequence used to determine how many
     * elements should be extracted from each priority level before proceeding
     * to the next one.
     */
    private final Enum<?>[] myPrioritySequence;

    /*
     * A data structure with each priority and the corresponding end-index in
     * the priority sequence. This is used as an optimization in order to find
     * an element to extract.
     */
    private final EnumMap<?, Integer> myPrioritiesWithEndSequenceIndexes;

    /*
     * A counter to used for circulating the defined priority levels.
     */
    private int myPriorityCirculation = 0;

    /**
     * Creates a {@link StripedBlockingQueue} with the provided priority levels,
     * initial capacity and the default access policy. The maximum total allowed
     * elements in this queue is {@link Integer#MAX_VALUE}.
     * 
     * @param weightsPerPriority
     *            How much (in percent) that should be spent on each priority
     *            level. The sum of these weights must be 100%.
     * @param initialQueueCapacities
     *            The initial capacity for each queue stripe. The internal
     *            queues will be initialized with this value, but capacity will
     *            increase if needed.
     * @throws IllegalArgumentException
     *             In case any input is illegal.
     */
    public <T extends Enum<T>> StripedBlockingQueue(
            EnumMap<T, Integer> weightsPerPriority, int initialQueueCapacities)
    {
        this(weightsPerPriority, initialQueueCapacities, false);
    }

    /**
     * Creates a {@link StripedBlockingQueue} with the provided priority levels,
     * initial capacity and the specified access policy. The maximum total
     * allowed elements in this queue is {@link Integer#MAX_VALUE}.
     * 
     * @param weightsPerPriority
     *            How much (in percent) that should be spent on each priority
     *            level. The sum of these weights must be 100%.
     * @param initialQueueCapacities
     *            The initial capacity for each queue stripe. The internal
     *            queues will be initialized with this value, but capacity will
     *            increase if needed.
     * @param fair
     *            If {@code true} then queue accesses for threads blocked on
     *            insertion or removal, are processed in FIFO order; if
     *            {@code false} the access order is unspecified.
     * @throws IllegalArgumentException
     *             In case any input is illegal.
     */
    public <T extends Enum<T>> StripedBlockingQueue(
            EnumMap<T, Integer> weightsPerPriority, int initialQueueCapacities,
            boolean fair)
    {
        this(weightsPerPriority, initialQueueCapacities, Integer.MAX_VALUE,
                fair);
    }

    /**
     * Creates a {@link StripedBlockingQueue} with the provided priority levels,
     * initial capacity and the specified access policy.
     * 
     * @param weightsPerPriority
     *            How much (in percent) that should be spent on each priority
     *            level. The sum of these weights must be 100%.
     * @param initialQueueCapacities
     *            The initial capacity for each queue stripe. The internal
     *            queues will be initialized with this value, but capacity will
     *            increase if needed.
     * @param totalCapacity
     *            The maximum total allowed capacity for this queue. It is not
     *            possible to change this limit after instantiation.
     * @param fair
     *            If {@code true} then queue accesses for threads blocked on
     *            insertion or removal, are processed in FIFO order; if
     *            {@code false} the access order is unspecified.
     * @throws IllegalArgumentException
     *             In case any input is illegal.
     */
    public <T extends Enum<T>> StripedBlockingQueue(
            EnumMap<T, Integer> weightsPerPriority, int initialQueueCapacities,
            int totalCapacity, boolean fair)
    {
        checkWeightsPerPriority(weightsPerPriority);

        myAccessPolicy = fair;
        myAccessLock = new ReentrantLock(myAccessPolicy);
        myNotEmpty = myAccessLock.newCondition();
        myNotFull = myAccessLock.newCondition();

        myTotalCapacity = totalCapacity;

        Class<T> priorityType = (Class<T>) weightsPerPriority.keySet()
                .iterator().next().getClass();
        EnumMap<T, ArrayDeque<E>> queues = new EnumMap<T, ArrayDeque<E>>(
                priorityType);
        EnumMap<T, Integer> prioritiesWithEndSequenceIndexes = new EnumMap<T, Integer>(
                priorityType);
        Enum<?>[] prioritySequence = new Enum<?>[PRIORITY_SEQUENCE_LENGTH];
        int prioritySequenceCounter = 0;

        for (Entry<T, Integer> entry : weightsPerPriority.entrySet())
        {
            T priority = entry.getKey();
            int weightPerPriority = entry.getValue() / 10;
            queues.put(priority, new ArrayDeque<E>(initialQueueCapacities));

            for (int i = 0; i < weightPerPriority; ++i)
            {
                prioritySequence[prioritySequenceCounter++] = priority;
            }

            prioritiesWithEndSequenceIndexes.put(priority,
                    prioritySequenceCounter - 1);
        }

        myQueues = queues;
        myPrioritySequence = prioritySequence;
        myPrioritiesWithEndSequenceIndexes = prioritiesWithEndSequenceIndexes;
    }

    /**
     * Inserts the specified element, waiting for space to become available if
     * the queue is full.
     * 
     * @param e
     *            The element to insert.
     * @throws InterruptedException
     *             If interrupted while waiting.
     * @throws ClassCastException
     *             In case the provided element is not of type
     *             {@link StripedQueueElement}.
     */
    @Override
    public void put(E e) throws InterruptedException
    {
        Enum<?> elementPriority = getQueueElement(e).getElementPriority();
        myAccessLock.lockInterruptibly();

        try
        {
            while (myCurrentSize == myTotalCapacity)
            {
                myNotFull.await();
            }

            insertElement(e, elementPriority);
        } finally
        {
            myAccessLock.unlock();
        }
    }

    /**
     * Inserts the specified element if it is possible to do so immediately
     * without exceeding the queue's capacity.
     * 
     * @param e
     *            The element to insert.
     * @return {@code true} if the element was inserted, otherwise {@code false}
     *         .
     * @throws ClassCastException
     *             In case the provided element is not of type
     *             {@link StripedQueueElement}.
     */
    @Override
    public boolean offer(E e)
    {
        Enum<?> elementPriority = getQueueElement(e).getElementPriority();
        myAccessLock.lock();

        try
        {
            if (myCurrentSize < myTotalCapacity)
            {
                insertElement(e, elementPriority);
                return true;
            }
        } finally
        {
            myAccessLock.unlock();
        }

        return false;
    }

    /**
     * Inserts the specified element, waiting up to the specified wait time for
     * space to become available if the queue is full.
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
     *             {@link StripedQueueElement}.
     */
    @Override
    public boolean offer(E e, long timeout, TimeUnit unit)
            throws InterruptedException
    {
        Enum<?> elementPriority = getQueueElement(e).getElementPriority();
        long nanos = unit.toNanos(timeout);
        myAccessLock.lockInterruptibly();

        try
        {
            for (;;)
            {
                if (myCurrentSize < myTotalCapacity)
                {
                    insertElement(e, elementPriority);
                    return true;
                }

                if (nanos <= 0)
                {
                    return false;
                }

                nanos = myNotFull.awaitNanos(nanos);
            }
        } finally
        {
            myAccessLock.unlock();
        }
    }

    /**
     * Retrieves and removes the head of this queue, waiting if necessary until
     * an element becomes available.
     * 
     * @return The head of this queue.
     * @throws InterruptedException
     *             If interrupted while waiting.
     */
    @Override
    public E take() throws InterruptedException
    {
        myAccessLock.lockInterruptibly();

        try
        {
            while (myCurrentSize == 0)
            {
                myNotEmpty.await();
            }

            return extractElement();
        } finally
        {
            myAccessLock.unlock();
        }
    }

    /**
     * Retrieves and removes the head of this queue, or returns {@code null} if
     * this queue is empty.
     * 
     * @return The head of this queue, or {@code null} if this queue is empty.
     */
    @Override
    public E poll()
    {
        myAccessLock.lock();

        try
        {
            return myCurrentSize > 0 ? extractElement() : null;
        } finally
        {
            myAccessLock.unlock();
        }
    }

    /**
     * Retrieves and removes the head of this queue, waiting up to the specified
     * wait time if necessary for an element to become available.
     * 
     * @param timeout
     *            How long to wait before giving up, in units of the provided
     *            {@link TimeUnit} parameter.
     * @param unit
     *            A {@link TimeUnit} determining how to interpret the timeout
     *            parameter.
     * @return The head of this queue, or {@code null} if the specified waiting
     *         time elapses before an element is available.
     * @throws InterruptedException
     *             If interrupted while waiting.
     */
    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException
    {
        long nanos = unit.toNanos(timeout);
        myAccessLock.lockInterruptibly();

        try
        {
            for (;;)
            {
                if (myCurrentSize > 0)
                {
                    return extractElement();
                }

                if (nanos <= 0)
                {
                    return null;
                }

                nanos = myNotEmpty.awaitNanos(nanos);
            }
        } finally
        {
            myAccessLock.unlock();
        }
    }

    /**
     * Retrieves, but does not remove, the head of this queue, or returns null
     * if this queue is empty.
     * 
     * @return The head of this queue, or {@code null} if this queue is empty.
     */
    @Override
    public E peek()
    {
        myAccessLock.lock();

        try
        {
            return myCurrentSize > 0 ? peekElement() : null;
        } finally
        {
            myAccessLock.unlock();
        }
    }

    /**
     * Returns the current number of elements in this queue.
     * 
     * @return The number of elements in this queue.
     */
    @Override
    public int size()
    {
        myAccessLock.lock();

        try
        {
            return myCurrentSize;
        } finally
        {
            myAccessLock.unlock();
        }
    }

    /**
     * Returns the number of additional elements that this queue can ideally (in
     * the absence of memory constraints) accept without blocking. This is
     * always equal to the total capacity of this queue less the current current
     * of this queue.
     * 
     * @return The remaining capacity.
     */
    @Override
    public int remainingCapacity()
    {
        return myTotalCapacity - myCurrentSize;
    }

    /**
     * Removes a single instance of the specified element from this queue, if it
     * is present. More formally, removes an element {@code e} such that
     * {@code o.equals(e)}, if this queue contains one or more such elements.
     * Returns {@code true} if this queue contained the specified element (or
     * equivalently, if this queue changed as a result of the call).
     * 
     * The internal queues in the {@link StripedBlockingQueue} are traversed in
     * the order in which their corresponding keys appear in the originally
     * provided {@link EnumMap}, which is their natural order (the order in
     * which the enum constants are declared).
     * 
     * Removal of interior elements in circular array based queues (as the
     * internal queues of the {@link StripedBlockingQueue} is an intrinsically
     * slow and disruptive operation, so should be undertaken only in
     * exceptional circumstances, ideally only when the queue is known not to be
     * accessible by other threads.
     * 
     * @param o
     *            The instance to remove.
     * @return {@code true} if an element was removed as a result of this call.
     * @throws ClassCastException
     *             In case the provided element is not of type
     *             {@link StripedQueueElement}.
     */
    @Override
    public boolean remove(Object o)
    {
        myAccessLock.lock();

        try
        {
            for (ArrayDeque<E> queue : myQueues.values())
            {
                if (queue.remove(o))
                {
                    --myCurrentSize;
                    myNotFull.signal();
                    return true;
                }
            }
        } finally
        {
            myAccessLock.unlock();
        }

        return false;
    }

    /**
     * Returns {@code true} if this queue contains the specified element. More
     * formally, returns {@code true} if and only if this queue contains at
     * least one element {@code e} such that {@code o.equals(e)}.
     * 
     * @param o
     *            The object to be checked for containment in this queue.
     * @return {@code true} if this queue contains the specified element.
     */
    @Override
    public boolean contains(Object object)
    {
        myAccessLock.lock();

        try
        {
            for (ArrayDeque<E> queue : myQueues.values())
            {
                if (queue.contains(object))
                {
                    return true;
                }
            }
        } finally
        {
            myAccessLock.unlock();
        }

        return false;
    }

    /**
     * Removes all available elements from this queue and adds them to the given
     * collection. This operation may be more efficient than repeatedly polling
     * this queue.
     * 
     * A failure encountered while attempting to add elements to the provided
     * collection may result in elements being in neither, either or both
     * collections when the associated exception is thrown. Due to the internal
     * mechanics of the {@link StripedBlockingQueue} it is highly recommended
     * that when encountering a failure while executing this method, a new
     * instance should replace this particular queue instance. It might not be
     * possible to fully recover from such a failure.
     * 
     * Attempts to drain a queue to itself result in
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
     *             If the provided collection is this queue.
     */
    @Override
    public int drainTo(Collection<? super E> collectionToDrainTo)
    {
        return drainTo(collectionToDrainTo, Integer.MAX_VALUE);
    }

    /**
     * Removes at most the provided number of elements from this queue and adds
     * them to the given collection. This operation may be more efficient than
     * repeatedly polling this queue.
     * 
     * A failure encountered while attempting to add elements to the provided
     * collection may result in elements being in neither, either or both
     * collections when the associated exception is thrown. Due to the internal
     * mechanics of the {@link StripedBlockingQueue} it is highly recommended
     * that when encountering a failure while executing this method, a new
     * instance should replace this particular queue instance. It might not be
     * possible to fully recover from such a failure.
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
     *             If the provided collection is this queue.
     */
    @Override
    public int drainTo(Collection<? super E> collectionToDrainTo,
            int maxElementsToDrain)
    {
        checkNotNull(collectionToDrainTo);
        checkNotSame(collectionToDrainTo, this);
        int numberOfDrainedElements = 0;
        myAccessLock.lock();

        try
        {
            E drainedElement;
            while (numberOfDrainedElements < maxElementsToDrain
                    && myCurrentSize > 0
                    && (drainedElement = extractElement()) != null)
            {
                collectionToDrainTo.add(drainedElement);
                ++numberOfDrainedElements;
            }

            if (numberOfDrainedElements > 0)
            {
                myNotFull.signalAll();
            }
        } finally
        {
            myAccessLock.unlock();
        }

        return numberOfDrainedElements;
    }

    /**
     * Removes all of the elements from this queue. The queue will be empty
     * after this call returns.
     * 
     */
    @Override
    public void clear()
    {
        myAccessLock.lock();

        try
        {
            if (myCurrentSize > 0)
            {
                while (myCurrentSize > 0)
                {
                    extractElement();
                }

                myNotFull.signalAll();
            }
        } finally
        {
            myAccessLock.unlock();
        }
    }

    /**
     * Returns an array containing all of the elements in this queue, in
     * sequence from high-to-low in terms of defined priority levels.
     * 
     * The returned array will be "safe" in that no references to it are
     * maintained by this queue. (In other words, this method must allocate a
     * new array). The caller is thus free to modify the returned array.
     * 
     * @return An array containing all of the elements in this queue.
     */
    @Override
    public Object[] toArray()
    {
        List<Object[]> arraysToConcatenate = Collections.emptyList();
        myAccessLock.lock();
        int currentSize = myCurrentSize;

        try
        {
            arraysToConcatenate = getQueuesAsArrays();
        } finally
        {
            myAccessLock.unlock();
        }

        Object[] a = new Object[currentSize];
        concatenateArrays(arraysToConcatenate, a);
        return a;
    }

    /**
     * Returns an array containing all of the elements in this queue, in
     * sequence from high-to-low in terms of defined priority levels. The
     * runtime type of the returned array is that of the provided array. If the
     * queue fits in the specified array, it is returned therein. Otherwise, a
     * new array is allocated with the runtime type of the specified array and
     * the size of this queue.
     * 
     * If this queue fits in the specified array with room to spare (i.e., the
     * array has more elements than this queue), the element in the array
     * immediately following the end of the queue is set to {@code null}
     * 
     * @param a
     *            The array into which the elements of the queue are to be
     *            stored, if it is big enough; otherwise, a new array of the
     *            same runtime type is allocated for this purpose.
     * @return An array containing all of the elements in this queue.
     * @throws ArrayStoreException
     *             If the runtime type of the specified array is not a supertype
     *             of the runtime type of every element in this queue.
     */
    @Override
    public <T> T[] toArray(T[] a)
    {
        List<Object[]> arraysToConcatenate = Collections.emptyList();
        myAccessLock.lock();
        int currentSize = myCurrentSize;

        try
        {
            if (a.length < currentSize)
            {
                a = (T[]) Array.newInstance(a.getClass().getComponentType(),
                        currentSize);
            }

            arraysToConcatenate = getQueuesAsArrays();
        } finally
        {
            myAccessLock.unlock();
        }

        concatenateArrays(arraysToConcatenate, a);

        if (a.length > currentSize)
        {
            a[currentSize] = null;
        }

        return a;
    }

    /**
     * Returns an iterator over the elements in this queue in sequence from
     * high-to-low in terms of defined priority levels. The returned
     * {@link Iterator} is a "weakly consistent" iterator that will never throw
     * {@link ConcurrentModificationException}, and guarantees to traverse
     * elements as they existed upon construction of the iterator, and may (but
     * is not guaranteed to) reflect any modifications subsequent to
     * construction.
     * 
     * @return An {@link Iterator} over the elements in this queue.
     */
    @Override
    public Iterator<E> iterator()
    {
        return new QueueIterator();
    }

    /**
     * Returns the internal queues as a list of arrays.
     * 
     * Must be called using mutual exclusion.
     * 
     * @return
     */
    private ArrayList<Object[]> getQueuesAsArrays()
    {
        ArrayList<Object[]> arraysToConcatenate = new ArrayList<Object[]>();

        for (ArrayDeque<E> queue : myQueues.values())
        {
            arraysToConcatenate.add(queue.toArray());
        }

        return arraysToConcatenate;
    }

    /**
     * Concatenates the contents of the provided list of arrays into the
     * destination array.
     * 
     * @param arraysToConcatenate
     * @param destination
     */
    private static <T> void concatenateArrays(List<T[]> arraysToConcatenate,
            T[] destination)
    {
        int nextStartIndexToUse = 0;

        for (T[] array : arraysToConcatenate)
        {
            System.arraycopy(array, 0, destination, nextStartIndexToUse,
                    array.length);
            nextStartIndexToUse += array.length;
        }
    }

    /**
     * Inserts the provided element into the queue.
     * 
     * Must be called using mutual exclusion.
     * 
     * @param element
     * @param elementPriority
     */
    private void insertElement(E element, Enum<?> elementPriority)
    {
        ArrayDeque<E> queue = myQueues.get(elementPriority);
        queue.addLast(element);
        ++myCurrentSize;
        myNotEmpty.signal();
    }

    /**
     * Extracts the head of the queue, which in theory could be the head of any
     * of the internal queues/stripes depending on the priority level
     * circulation.
     * 
     * Must be called using mutual exclusion.
     * 
     * @return
     */
    private E extractElement()
    {
        for (int i = 0; i < myPrioritiesWithEndSequenceIndexes.size(); ++i)
        {
            Enum<?> currentPriorityToPoll = myPrioritySequence[myPriorityCirculation];
            ArrayDeque<E> queue = myQueues.get(currentPriorityToPoll);
            E element = queue.poll();

            if (element != null)
            {
                myPriorityCirculation = stepPriorityCirculation(myPriorityCirculation);
                --myCurrentSize;
                myNotFull.signal();
                return element;
            }

            int currentlyPolledPriorityEndIndex = myPrioritiesWithEndSequenceIndexes
                    .get(currentPriorityToPoll);
            myPriorityCirculation = stepPriorityCirculationToNextLevel(currentlyPolledPriorityEndIndex);
        }

        // Must never reach this point - caller must make sure elements exist in
        // queue
        throw new NoSuchElementException();
    }

    /**
     * Peeks the head of the queue, which in theory could be the head of any of
     * the internal queues depending on the priority level circulation.
     * 
     * Must be called using mutual exclusion.
     * 
     * @return
     */
    private E peekElement()
    {
        int priorityCirculation = myPriorityCirculation;
        for (int i = 0; i < myPrioritiesWithEndSequenceIndexes.size(); ++i)
        {
            Enum<?> currentPriorityToPoll = myPrioritySequence[priorityCirculation];
            ArrayDeque<E> queue = myQueues.get(currentPriorityToPoll);
            E element = queue.poll();

            if (element != null)
            {
                return element;
            }

            int currentlyPolledPriorityEndIndex = myPrioritiesWithEndSequenceIndexes
                    .get(currentPriorityToPoll);
            priorityCirculation = stepPriorityCirculationToNextLevel(currentlyPolledPriorityEndIndex);
        }

        // Must never reach this point - caller must make sure elements exist in
        // queue
        throw new NoSuchElementException();
    }

    /**
     * Steps the priority circulation to the next priority element to poll. This
     * method is static and takes the queue's state as arguments.
     * 
     * @param currentPriorityCirculatioValue
     */
    private static int stepPriorityCirculation(
            int currentPriorityCirculatioValue)
    {
        return (++currentPriorityCirculatioValue == PRIORITY_SEQUENCE_LENGTH) ? 0
                : currentPriorityCirculatioValue;
    }

    /**
     * Fast-forwards the priority circulation to the next priority level to
     * poll. This method is static and takes the queue's state as arguments.
     * 
     * @param currentlyPolledPriorityEndIndex
     */
    private static int stepPriorityCirculationToNextLevel(
            int currentlyPolledPriorityEndIndex)
    {
        return (currentlyPolledPriorityEndIndex == (PRIORITY_SEQUENCE_LENGTH - 1)) ? 0
                : (currentlyPolledPriorityEndIndex + 1);
    }

    /**
     * Casts the provided object into a {@link StripedQueueElement}, or throws a
     * {@link ClassCastException} if not possible.
     * 
     * @param o
     * @return
     */
    private static StripedQueueElement getQueueElement(Object o)
    {
        return (StripedQueueElement) o;
    }

    /**
     * Verifies that the provided object is not null, and if it is a
     * {@link NullPointerException} is thrown.
     * 
     * @param object
     */
    private static void checkNotNull(Object object)
    {
        if (object == null)
        {
            throw new NullPointerException();
        }
    }

    /**
     * Verifies that the two references don't refer to the very same object, and
     * if they do an {@link IllegalArgumentException} is thrown.
     * 
     * @param object
     */
    private static void checkNotSame(Object first, Object second)
    {
        if (first == second)
        {
            throw new IllegalArgumentException("Not allowed due to same object");
        }
    }

    /**
     * Performs a sanity-check on the provided {@link EnumMap}. Might throw
     * {@link IllegalArgumentException} in case input is illegal.
     * 
     * @param weightPerPriority
     */
    private static <T extends Enum<T>> void checkWeightsPerPriority(
            EnumMap<T, Integer> weightPerPriority)
    {
        if (weightPerPriority.isEmpty())
        {
            throw new IllegalArgumentException(
                    "At least one priority level with an assigned weight must be defined");
        }

        int sum = 0;
        for (Entry<T, Integer> entry : weightPerPriority.entrySet())
        {
            T priority = entry.getKey();
            int weight = entry.getValue();

            if (weight % 10 != 0)
            {
                throw new IllegalArgumentException("Weight for " + priority
                        + " must be expressed in tenth(s) of percent (not "
                        + weight + "%)");
            }

            sum += weight;
        }

        if (sum != 100)
        {
            throw new IllegalArgumentException(
                    "The sum of all priority level weights must be 100% (not "
                            + sum + "%)");
        }
    }

    /**
     * An {@link Iterator} implementation which takes a combined snapshot of all
     * the underlying queues/stripes.
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
            myArray = toArray();
        }

        @Override
        public boolean hasNext()
        {
            return myCurrentIndex < myArray.length;
        }

        @Override
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
         * Identity-based removal of the provided object. This method will be
         * executed using mutual exclusion.
         * 
         * @param objectToRemove
         */
        private void removeElementByIdentity(Object objectToRemove)
        {
            myAccessLock.lock();

            try
            {
                queueLoop: for (ArrayDeque<E> queue : myQueues.values())
                {
                    Iterator<E> iterator = queue.iterator();

                    while (iterator.hasNext())
                    {
                        E element = iterator.next();

                        if (element == objectToRemove)
                        {
                            iterator.remove();
                            --myCurrentSize;
                            myNotFull.signal();
                            break queueLoop;
                        }
                    }
                }
            } finally
            {
                myAccessLock.unlock();
            }
        }
    }
}
