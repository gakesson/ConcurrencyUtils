package gakesson.util.concurrent;

import static org.fest.assertions.Assertions.assertThat;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test class to verify the {@link StripedBlockingQueue}.
 * 
 * The private methods prefixed with "concurrent" is a bit verbose, but is so in
 * order to get reliable results without sleeping. Instead the verification is
 * performed using signaling with locks and barriers.
 * 
 */
public class StripedBlockingQueueTest
{
    private static final int QUEUE_CAPACITY = 100;

    private ExecutorService myExecutor;

    @BeforeMethod
    public void initTestCase()
    {
        myExecutor = Executors.newCachedThreadPool();
    }

    @AfterMethod
    public void tearDownTestCase()
    {
        myExecutor.shutdownNow();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenProvidingNoPriorityWeightsToQueue()
    {
        EnumMap<QueuePriority, Integer> noWeightsPerPriority = new EnumMap<QueuePriority, Integer>(
                QueuePriority.class);
        new StripedBlockingQueue<Element>(noWeightsPerPriority, QUEUE_CAPACITY,
                QUEUE_CAPACITY, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenProvidingPriorityWeightsWhichExceedOneHundred()
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = new EnumMap<QueuePriority, Integer>(
                QueuePriority.class);
        weightPerPriority.put(QueuePriority.LOW, 10);
        weightPerPriority.put(QueuePriority.MEDIUM, 20);
        weightPerPriority.put(QueuePriority.HIGH, 80);
        new StripedBlockingQueue<Element>(weightPerPriority, QUEUE_CAPACITY);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenProvidingPriorityWeightWhichIsNotDivisibleByTen()
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = new EnumMap<QueuePriority, Integer>(
                QueuePriority.class);
        weightPerPriority.put(QueuePriority.LOW, 15);
        new StripedBlockingQueue<Element>(weightPerPriority, QUEUE_CAPACITY);
    }

    @Test
    public void shouldAddAllElements()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();

        verifyAndAddAllElementsForAllPriorities(queue, QUEUE_CAPACITY);

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
    }

    @Test
    public void shouldOfferAllElements()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();

        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
    }

    @Test
    public void shouldSuccessfullyOfferAllElementsUsingASpecifiedTimeout()
            throws InterruptedException
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();

        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY, 10,
                TimeUnit.SECONDS);

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
    }

    @Test
    public void shouldSuccessfullyPutAllElementsUsingASpecifiedTimeout()
            throws InterruptedException
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();

        putAllElementsForAllPriorities(queue, QUEUE_CAPACITY);

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
    }

    @Test
    public void shouldNotOfferElementWhenViolatingQueueCapacity()
    {
        int maximumAllowedElements = QUEUE_CAPACITY;
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, maximumAllowedElements,
                maximumAllowedElements, false);

        verifyAndOfferAllElements(queue, maximumAllowedElements,
                QueuePriority.MEDIUM);
        boolean inserted = queue.offer(new Element(QueuePriority.HIGH));

        assertThat(inserted).isFalse();
        assertThat(queue).hasSize(maximumAllowedElements);
    }

    @Test
    public void shouldNotOfferElementWhenViolatingQueueCapacityAndSpecifyingAZeroTimeout()
            throws InterruptedException
    {
        int maximumAllowedElements = QUEUE_CAPACITY;
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, maximumAllowedElements,
                maximumAllowedElements, false);

        verifyAndOfferAllElements(queue, maximumAllowedElements,
                QueuePriority.MEDIUM);
        boolean inserted = queue.offer(new Element(QueuePriority.HIGH), 0,
                TimeUnit.SECONDS);

        assertThat(inserted).isFalse();
        assertThat(queue).hasSize(maximumAllowedElements);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldThrowNullPointerExceptionWhenOfferNullValue()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        queue.offer(null);
    }

    @Test
    public void shouldPollNullWhenQueueIsEmpty()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();

        Element element = queue.poll();

        assertThat(element).isNull();
    }

    @Test
    public void shouldPollAllFromQueue()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        int numberOfPolledElements = 0;

        while (!queue.isEmpty())
        {
            numberOfPolledElements++;
            Element element = queue.poll();
            assertThat(element).isNotNull();
        }

        assertThat(numberOfPolledElements).isEqualTo(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
        assertThat(queue).isEmpty();
    }

    @Test
    public void shouldSuccessfullyPollAllUsingASpecifiedTimeout()
            throws InterruptedException
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        int numberOfPolledElements = 0;

        while (!queue.isEmpty())
        {
            numberOfPolledElements++;
            Element element = queue.poll(1, TimeUnit.SECONDS);
            assertThat(element).isNotNull();
        }

        assertThat(numberOfPolledElements).isEqualTo(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
        assertThat(queue).isEmpty();
    }

    @Test
    public void shouldNotPollMoreElementsThanAllowedWhenUsingAZeroTimeout()
            throws InterruptedException
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        int numberOfPolledElements = 0;

        while (!queue.isEmpty())
        {
            numberOfPolledElements++;
            queue.poll();
        }

        Element element = queue.poll(0, TimeUnit.SECONDS);

        assertThat(element).isNull();
        assertThat(numberOfPolledElements).isEqualTo(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
        assertThat(queue).isEmpty();
    }

    @Test
    public void shouldDrainQueue()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        BlockingQueue<Element> anotherQueue = new StripedBlockingQueue<Element>(
                createDefaultWeightPerPriority(), QUEUE_CAPACITY);

        queue.drainTo(anotherQueue);

        assertThat(queue).isEmpty();
        assertThat(anotherQueue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
    }

    @Test
    public void shouldDrainQueueButOnlyToMaxElements()
    {
        int maxElementsToDrain = calculateSizeWhenQueuesInAllPrioritiesAreFull() / 2;
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        BlockingQueue<Element> anotherQueue = new StripedBlockingQueue<Element>(
                createDefaultWeightPerPriority(), QUEUE_CAPACITY);

        queue.drainTo(anotherQueue, maxElementsToDrain);

        assertThat(queue).hasSize(maxElementsToDrain);
        assertThat(anotherQueue).hasSize(maxElementsToDrain);
    }

    @Test
    public void shouldNotDrainQueueWhenMaxElementsAreZero()
    {
        int maxElementsToDrain = 0;
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        BlockingQueue<Element> anotherQueue = new StripedBlockingQueue<Element>(
                createDefaultWeightPerPriority(), QUEUE_CAPACITY);

        queue.drainTo(anotherQueue, maxElementsToDrain);

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
        assertThat(anotherQueue).isEmpty();
    }

    @Test
    public void shouldCopeWithDrainingEmptyQueue()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        BlockingQueue<Element> anotherQueue = new StripedBlockingQueue<Element>(
                createDefaultWeightPerPriority(), QUEUE_CAPACITY);

        queue.drainTo(anotherQueue);

        assertThat(queue).isEmpty();
        assertThat(anotherQueue).isEmpty();
    }

    @Test
    public void shouldCopeWithDrainingEmptyQueueUsingMaxElements()
    {
        int maxElementsToDrain = calculateSizeWhenQueuesInAllPrioritiesAreFull() / 2;
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        BlockingQueue<Element> anotherQueue = new StripedBlockingQueue<Element>(
                createDefaultWeightPerPriority(), QUEUE_CAPACITY);

        queue.drainTo(anotherQueue, maxElementsToDrain);

        assertThat(queue).isEmpty();
        assertThat(anotherQueue).isEmpty();
    }

    @Test
    public void shouldCopeWithDrainingQueueWhichSizeIsLessThanMaxElements()
    {
        int maxElementsToDrain = calculateSizeWhenQueuesInAllPrioritiesAreFull() * 2;
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        BlockingQueue<Element> anotherQueue = new StripedBlockingQueue<Element>(
                createDefaultWeightPerPriority(), QUEUE_CAPACITY);

        queue.drainTo(anotherQueue, maxElementsToDrain);

        assertThat(queue).isEmpty();
        assertThat(anotherQueue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenThisQueueAndDrainToQueueAreSame()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();

        queue.drainTo(queue);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenThisQueueAndDrainToQueueAreSameAndUsingMaxElements()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();

        queue.drainTo(queue, calculateSizeWhenQueuesInAllPrioritiesAreFull());
    }

    @Test
    public void shouldThrowIllegalStateExceptionWhenAddingElementButViolatingCapacity()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);

        for (QueuePriority queuePriority : QueuePriority.values())
        {
            try
            {
                queue.add(new Element(queuePriority));
                fail("Should have thrown IllegalStateException");
            } catch (IllegalStateException e)
            {
                // Expected
            }
        }

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldThrowNullPointerExceptionWhenAddingNullValue()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();

        queue.add(null);
    }

    @Test
    public void shouldIterateOverQueueElements()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        Set<Element> identityBasedSet = Collections
                .newSetFromMap(new IdentityHashMap<Element, Boolean>());
        int numberOfIteratedElements = 0;

        for (Element element : queue)
        {
            identityBasedSet.add(element);
            numberOfIteratedElements++;

            if (!queue.contains(element))
            {
                fail("Queue should have contained element");
            }
        }

        assertThat(identityBasedSet).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
        assertThat(numberOfIteratedElements).isEqualTo(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void shouldThrowNoSuchElementExceptionWhenAttemptingToInvokeNextOnIteratorFromAnEmptyQueue()
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        Iterator<Element> iterator = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY).iterator();
        iterator.next();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionWhenAttemptingToInvokeRemoveOnCompletelyNewIterator()
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        StripedBlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);

        Iterator<Element> iterator = queue.iterator();
        iterator.remove();
    }

    @Test
    public void shouldRemoveElementsFromQueueUsingIterator()
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        StripedBlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);

        Iterator<Element> iterator = queue.iterator();
        int numberOfIteratedElements = 0;

        while (iterator.hasNext())
        {
            numberOfIteratedElements++;
            iterator.next();
            iterator.remove();
        }

        assertThat(numberOfIteratedElements).isEqualTo(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
        assertThat(queue).isEmpty();
    }

    @Test
    public void shouldPeekAtElementWithHighPriority()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);

        Element element = queue.peek();

        assertThat(element.getElementPriority()).isEqualTo(QueuePriority.HIGH);
    }

    @Test
    public void shouldPeekAtElementWithMediumPriority()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElements(queue, QUEUE_CAPACITY, QueuePriority.MEDIUM);

        Element element = queue.peek();

        assertThat(element.getElementPriority())
                .isEqualTo(QueuePriority.MEDIUM);
    }

    @Test
    public void shouldPeekAtElementWithLowPriority()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElements(queue, QUEUE_CAPACITY, QueuePriority.LOW);

        Element element = queue.peek();

        assertThat(element.getElementPriority()).isEqualTo(QueuePriority.LOW);
    }

    @Test
    public void shouldPeekAtNullWhenEmptyQueue()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();

        Element element = queue.peek();

        assertThat(element).isNull();
    }

    @Test
    public void shouldCalculateNoRemainingCapacity()
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY,
                calculateSizeWhenQueuesInAllPrioritiesAreFull(), false);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);

        assertThat(queue.remainingCapacity()).isZero();
    }

    @Test
    public void shouldCalculateRemainingCapacity()
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY,
                calculateSizeWhenQueuesInAllPrioritiesAreFull(), false);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY / 2);

        assertThat(queue.remainingCapacity()).isEqualTo(
                calculateSizeWhenQueuesInAllPrioritiesAreFull() / 2);
    }

    @Test
    public void shouldReturnCurrentSizeOfQueue()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY / 2);

        int numberOfInsertedElements = calculateSizeWhenQueuesInAllPrioritiesAreFull() / 2;
        int expectedSize = calculateSizeWhenQueuesInAllPrioritiesAreFull()
                - numberOfInsertedElements;

        assertThat(queue).hasSize(expectedSize);
    }

    @Test
    public void shouldReturnZeroWhenQueueIsEmpty()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();

        assertThat(queue).isEmpty();
    }

    @Test
    public void shouldRemoveElementsByEquality()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        Iterator<Element> createdElements = verifyAndOfferAllElementsForAllPriorities(
                queue, QUEUE_CAPACITY).iterator();
        int numberOfRemovals = 0;

        while (!queue.isEmpty())
        {
            boolean removed = queue.remove(createdElements.next());
            numberOfRemovals++;

            if (!removed)
            {
                fail("Should have removed the specified element");
            }
        }

        assertThat(queue).isEmpty();
        assertThat(numberOfRemovals).isEqualTo(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
        assertThat(createdElements.hasNext()).isFalse();
    }

    @Test
    public void shouldNotRemoveNonExistingElement()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);

        boolean removed = queue.remove(new Element("Dummy",
                QueuePriority.MEDIUM));

        assertThat(removed).isFalse();
        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
    }

    @Test
    public void shouldReturnArrayRepresentationOfQueue()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        List<Element> createdElements = verifyAndOfferAllElementsForAllPriorities(
                queue, QUEUE_CAPACITY);

        Element[] destination = new Element[queue.size()];
        Element[] elements = queue.toArray(destination);

        assertThat(elements).hasSize(createdElements.size());
        assertThat(createdElements.containsAll(Arrays.asList(elements)));
        assertThat(destination).isSameAs(elements);
    }

    @Test
    public void shouldReturnArrayRepresentationOfQueueUsingNoArguments()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        List<Element> createdElements = verifyAndOfferAllElementsForAllPriorities(
                queue, QUEUE_CAPACITY);

        Object[] elements = queue.toArray();

        assertThat(elements).hasSize(createdElements.size());
        assertThat(createdElements.containsAll(Arrays.asList(elements)));
    }

    @Test
    public void shouldClearElementsInQueue()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);

        assertThat(queue).isNotEmpty();

        queue.clear();

        assertThat(queue).isEmpty();
    }

    @Test
    public void shouldContainElement()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY / 2);
        Element element = new Element("Dummy", QueuePriority.MEDIUM);
        queue.add(element);

        boolean exists = queue.contains(element);

        assertThat(exists).isTrue();
    }

    @Test
    public void shouldNotContainNull()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);

        boolean exists = queue.contains(null);

        assertThat(exists).isFalse();
    }

    @Test
    public void shouldNotBeAbleToRemoveNull()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);

        boolean removed = queue.remove(null);

        assertThat(removed).isFalse();
    }

    @Test
    public void shouldReturnArrayRepresentationOfQueueWhenProvidingArrayWhichIsGreaterThanQueueSize()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        int newArraySize = queue.size() * 2;

        Element[] elements = queue.toArray(new Element[newArraySize]);

        assertThat(elements).hasSize(newArraySize);
        assertThat(elements[queue.size()]).isNull();
    }

    @Test
    public void shouldReturnArrayRepresentationOfQueueByProvidingSizeLessThanQueueSize()
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        List<Element> createdElements = verifyAndOfferAllElementsForAllPriorities(
                queue, QUEUE_CAPACITY);

        Element[] destination = new Element[queue.size() / 2];
        Element[] elements = queue.toArray(destination);

        assertThat(elements).hasSize(createdElements.size());
        assertThat(createdElements.containsAll(Arrays.asList(elements)));
    }

    @Test
    public void shouldTakeAllFromQueue() throws InterruptedException
    {
        BlockingQueue<Element> queue = createDefaultStripedBlockingQueue();
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);

        int numberOfPolledElements = 0;

        while (!queue.isEmpty())
        {
            numberOfPolledElements++;
            Element element = queue.take();
            assertThat(element).isNotNull();
        }

        assertThat(numberOfPolledElements).isEqualTo(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());
        assertThat(queue).isEmpty();
    }

    @Test
    public void shouldOfferElementToFullQueueUntilQueueIsCleared()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.clear();
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurentPerformOfferElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldOfferElementToFullQueueUntilQueueIsDrained()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.drainTo(new ArrayList<Element>());
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurentPerformOfferElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldOfferElementToFullQueueUntilQueueIsDrainedUsingMaxElements()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                int maxElements = queue.size() * 2;
                queue.drainTo(new ArrayList<Element>(), maxElements);
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurentPerformOfferElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldOfferElementToFullQueueUntilAnotherElementIsRemoved()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        final Element firstElement = verifyAndOfferAllElementsForAllPriorities(
                queue, QUEUE_CAPACITY).iterator().next();
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.remove(firstElement);
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurentPerformOfferElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldOfferElementToFullQueueUntilAnotherElementIsRemovedUsingIterator()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                Iterator<Element> iterator = queue.iterator();
                iterator.next();
                iterator.remove();
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurentPerformOfferElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldOfferElementToFullQueueUntilAnotherElementIsPolled()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.poll();
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurentPerformOfferElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldOfferElementToFullQueueUntilAnotherElementIsPolledUsingSpecifiedTimeout()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.poll(10, TimeUnit.SECONDS);
                } catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurentPerformOfferElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldOfferElementToFullQueueUntilAnotherElementIsTaken()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.take();
                } catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurentPerformOfferElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldPutElementToFullQueueUntilQueueIsCleared()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.clear();
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurrentPerformPutElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldPutElementToFullQueueUntilQueueIsDrained()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.drainTo(new ArrayList<Element>());
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurrentPerformPutElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldPutElementToFullQueueUntilQueueIsDrainedUsingMaxElements()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                int maxElements = queue.size() * 2;
                queue.drainTo(new ArrayList<Element>(), maxElements);
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurrentPerformPutElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldPutElementToFullQueueUntilAnotherElementIsRemoved()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        final Element firstElement = verifyAndOfferAllElementsForAllPriorities(
                queue, QUEUE_CAPACITY).iterator().next();
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.remove(firstElement);
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurrentPerformPutElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldPutElementToFullQueueUntilAnotherElementIsRemovedUsingIterator()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                Iterator<Element> iterator = queue.iterator();
                iterator.next();
                iterator.remove();
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurrentPerformPutElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldPutElementToFullQueueUntilAnotherElementIsPolled()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.poll();
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurrentPerformPutElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldPutElementToFullQueueUntilAnotherElementIsPolledUsingSpecifiedTimeout()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.poll(10, TimeUnit.SECONDS);
                } catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurrentPerformPutElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldPutElementToFullQueueUntilAnotherElementIsTaken()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        verifyAndOfferAllElementsForAllPriorities(queue, QUEUE_CAPACITY);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.take();
                } catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue).hasSize(
                calculateSizeWhenQueuesInAllPrioritiesAreFull());

        concurrentPerformPutElementUntilSpaceIsAvailable(
                executionToEnableSpace, queue);
    }

    @Test
    public void shouldPollElementFromEmptyQueueUntilElementIsOffered()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        final Element elementToAdd = new Element(QueuePriority.MEDIUM);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.offer(elementToAdd);
            }
        };

        assertThat(queue).isEmpty();

        concurrentPerformPollUntilElementIsAvailable(executionToEnableSpace,
                queue, elementToAdd);
    }

    @Test
    public void shouldPollElementFromEmptyQueueUntilElementIsOfferedUsingSpecifiedTimeout()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        final Element elementToAdd = new Element(QueuePriority.MEDIUM);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.offer(elementToAdd, 10, TimeUnit.SECONDS);
                } catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue).isEmpty();

        concurrentPerformPollUntilElementIsAvailable(executionToEnableSpace,
                queue, elementToAdd);
    }

    @Test
    public void shouldPollElementFromEmptyQueueUntilElementIsPut()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        final Element elementToAdd = new Element(QueuePriority.MEDIUM);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.put(elementToAdd);
                } catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue).isEmpty();

        concurrentPerformPollUntilElementIsAvailable(executionToEnableSpace,
                queue, elementToAdd);
    }

    @Test
    public void shouldPollElementFromEmptyQueueUntilElementIsAdded()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        final Element elementToAdd = new Element(QueuePriority.MEDIUM);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.add(elementToAdd);
            }
        };

        assertThat(queue).isEmpty();

        concurrentPerformPollUntilElementIsAvailable(executionToEnableSpace,
                queue, elementToAdd);
    }

    @Test
    public void shouldTakeElementFromEmptyQueueUntilElementIsOffered()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        final Element elementToAdd = new Element(QueuePriority.MEDIUM);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.offer(elementToAdd);
            }
        };

        assertThat(queue).isEmpty();

        concurrentPerformTakeUntilElementIsAvailable(executionToEnableSpace,
                queue, elementToAdd);
    }

    @Test
    public void shouldTakeElementFromEmptyQueueUntilElementIsOfferedUsingSpecifiedTimeout()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        final Element elementToAdd = new Element(QueuePriority.MEDIUM);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.offer(elementToAdd, 10, TimeUnit.SECONDS);
                } catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue).isEmpty();

        concurrentPerformTakeUntilElementIsAvailable(executionToEnableSpace,
                queue, elementToAdd);
    }

    @Test
    public void shouldTakeElementFromEmptyQueueUntilElementIsPut()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        final Element elementToAdd = new Element(QueuePriority.MEDIUM);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.put(elementToAdd);
                } catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue).isEmpty();

        concurrentPerformTakeUntilElementIsAvailable(executionToEnableSpace,
                queue, elementToAdd);
    }

    @Test
    public void shouldTakeElementFromEmptyQueueUntilElementIsAdded()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queue = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        final Element elementToAdd = new Element(QueuePriority.MEDIUM);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.add(elementToAdd);
            }
        };

        assertThat(queue).isEmpty();

        concurrentPerformTakeUntilElementIsAvailable(executionToEnableSpace,
                queue, elementToAdd);
    }

    @Test
    public void shouldTakeElementsAccordingToWeightsWhenElementsExistForAllPriorityLevels()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queueToUse = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        int numberOfElements = 30;

        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.LOW));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));

        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.LOW));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));

        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.LOW));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));

        for (int i = 0; i < numberOfElements; ++i)
        {
            Element element = queueToUse.take();
            if (i % (numberOfElements / 3) < 6)
            {
                assertThat(element.getElementPriority()).isEqualTo(
                        QueuePriority.HIGH);
            } else if (i % (numberOfElements / 3) < 9)
            {
                assertThat(element.getElementPriority()).isEqualTo(
                        QueuePriority.MEDIUM);
            } else if (i % (numberOfElements / 3) < 10)
            {
                assertThat(element.getElementPriority()).isEqualTo(
                        QueuePriority.LOW);
            } else
            {
                fail("Not expected");
            }
        }

        assertThat(queueToUse).isEmpty();
    }

    @Test
    public void shouldTakeElementsAccordingToWeightsWhenOnlyMediumPriorityLevelElementsExist()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queueToUse = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        int numberOfElements = 50;

        for (int i = 0; i < numberOfElements; ++i)
        {
            queueToUse.offer(new Element(QueuePriority.MEDIUM));
        }

        for (int i = 0; i < numberOfElements; ++i)
        {
            Element element = queueToUse.take();
            assertThat(element.getElementPriority()).isEqualTo(
                    QueuePriority.MEDIUM);
        }

        assertThat(queueToUse).isEmpty();
    }

    @Test
    public void shouldTakeElementsAccordingToWeightsWhenNoLowPriorityLevelElementsExist()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queueToUse = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        int numberOfElements = 27;

        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));

        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));

        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.HIGH));

        for (int i = 0; i < numberOfElements; ++i)
        {
            Element element = queueToUse.take();

            if (i % (numberOfElements / 3) < 6)
            {
                assertThat(element.getElementPriority()).isEqualTo(
                        QueuePriority.HIGH);
            } else if (i % (numberOfElements / 3) < 10)
            {
                assertThat(element.getElementPriority()).isEqualTo(
                        QueuePriority.MEDIUM);
            } else
            {
                fail("Not expected");
            }
        }

        assertThat(queueToUse).isEmpty();
    }

    @Test
    public void shouldTakeElementsAccordingToWeightsWhenNoMediumPriorityLevelElementsExist()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queueToUse = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        int numberOfElements = 21;

        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.LOW));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));

        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.LOW));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));

        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.LOW));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));
        queueToUse.offer(new Element(QueuePriority.HIGH));

        for (int i = 0; i < numberOfElements; ++i)
        {
            Element element = queueToUse.take();

            if (i % (numberOfElements / 3) < 6)
            {
                assertThat(element.getElementPriority()).isEqualTo(
                        QueuePriority.HIGH);
            } else if (i % (numberOfElements / 3) < 7)
            {
                assertThat(element.getElementPriority()).isEqualTo(
                        QueuePriority.LOW);
            } else
            {
                fail("Not expected");
            }
        }

        assertThat(queueToUse).isEmpty();
    }

    @Test
    public void shouldTakeElementsAccordingToWeightsWhenNoHighPriorityLevelElementsExist()
            throws Exception
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        final BlockingQueue<Element> queueToUse = new StripedBlockingQueue<Element>(
                weightPerPriority, QUEUE_CAPACITY);
        int numberOfElements = 12;

        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.LOW));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));

        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.LOW));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));

        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.LOW));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));
        queueToUse.offer(new Element(QueuePriority.MEDIUM));

        for (int i = 0; i < numberOfElements; ++i)
        {
            Element element = queueToUse.take();
            if (i % (numberOfElements / 3) < 3)
            {
                assertThat(element.getElementPriority()).isEqualTo(
                        QueuePriority.MEDIUM);
            } else if (i % (numberOfElements / 3) < 4)
            {
                assertThat(element.getElementPriority()).isEqualTo(
                        QueuePriority.LOW);
            } else
            {
                fail("Not expected");
            }
        }

        assertThat(queueToUse).isEmpty();
    }

    /**
     * Concurrently performs take element until space becomes available in the
     * provided queue. This execution which will enable space is executed via
     * the provided {@link ExecutionInjection}.
     * 
     * @param executionToAddElementToQueue
     * @param queue
     * @param elementToBeAdded
     * @throws Exception
     */
    private void concurrentPerformTakeUntilElementIsAvailable(
            ExecutionInjection executionToAddElementToQueue,
            final BlockingQueue<Element> queue, Element elementToBeAdded)
            throws Exception
    {
        final AtomicReference<Element> takenElement = new AtomicReference<Element>();
        final CountDownLatch reachedLockBarrier = new CountDownLatch(1);
        final CountDownLatch takenElementBarrier = new CountDownLatch(1);
        final ReentrantLock internalLock = extractInternalLockFrom(queue);
        myExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                internalLock.lock();
                try
                {
                    reachedLockBarrier.countDown();
                    Element element = queue.take();
                    takenElement.set(element);
                    takenElementBarrier.countDown();
                } catch (InterruptedException e)
                {
                    // No point in doing anything...
                } finally
                {
                    internalLock.unlock();
                }
            }
        });

        reachedLockBarrier.await(10, TimeUnit.SECONDS);
        internalLock.lock();

        try
        {
            executionToAddElementToQueue.apply();
        } finally
        {
            internalLock.unlock();
        }

        takenElementBarrier.await(2, TimeUnit.SECONDS);

        assertThat(takenElement.get()).isSameAs(elementToBeAdded);
    }

    /**
     * Concurrently performs poll element until space becomes available in the
     * provided queue. This execution which will enable space is executed via
     * the provided {@link ExecutionInjection}.
     * 
     * @param executionToAddElementToQueue
     * @param queue
     * @param elementToBeAdded
     * @throws Exception
     */
    private void concurrentPerformPollUntilElementIsAvailable(
            ExecutionInjection executionToAddElementToQueue,
            final BlockingQueue<Element> queue, Element elementToBeAdded)
            throws Exception
    {
        final AtomicReference<Element> polledElement = new AtomicReference<Element>();
        final CountDownLatch reachedLockBarrier = new CountDownLatch(1);
        final CountDownLatch polledElementBarrier = new CountDownLatch(1);
        final ReentrantLock internalLock = extractInternalLockFrom(queue);
        myExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                internalLock.lock();
                try
                {
                    reachedLockBarrier.countDown();
                    Element element = queue.poll(30, TimeUnit.SECONDS);
                    polledElement.set(element);
                    polledElementBarrier.countDown();
                } catch (InterruptedException e)
                {
                    // No point in doing anything...
                } finally
                {
                    internalLock.unlock();
                }
            }
        });

        reachedLockBarrier.await(10, TimeUnit.SECONDS);
        internalLock.lock();

        try
        {
            executionToAddElementToQueue.apply();
        } finally
        {
            internalLock.unlock();
        }

        polledElementBarrier.await(2, TimeUnit.SECONDS);

        assertThat(polledElement.get()).isSameAs(elementToBeAdded);
    }

    /**
     * Concurrently performs offer element until space becomes available in the
     * provided queue. This execution which will enable space is executed via
     * the provided {@link ExecutionInjection}.
     * 
     * @param executionToEnableSpace
     * @param queue
     * @throws Exception
     */
    private void concurentPerformOfferElementUntilSpaceIsAvailable(
            ExecutionInjection executionToEnableSpace,
            final BlockingQueue<Element> queue) throws Exception
    {
        final AtomicBoolean offeredElement = new AtomicBoolean(false);
        final CountDownLatch reachedLockBarrier = new CountDownLatch(1);
        final CountDownLatch offeredElementBarrier = new CountDownLatch(1);
        final ReentrantLock internalLock = extractInternalLockFrom(queue);
        final Element elementToOffer = new Element(QueuePriority.MEDIUM);
        myExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                internalLock.lock();
                try
                {
                    reachedLockBarrier.countDown();
                    boolean isInserted = queue.offer(elementToOffer, 30,
                            TimeUnit.SECONDS);
                    offeredElement.set(isInserted);
                    offeredElementBarrier.countDown();
                } catch (InterruptedException e)
                {
                    // No point in doing anything...
                } finally
                {
                    internalLock.unlock();
                }
            }
        });

        reachedLockBarrier.await(10, TimeUnit.SECONDS);
        internalLock.lock();

        try
        {
            executionToEnableSpace.apply();
        } finally
        {
            internalLock.unlock();
        }

        offeredElementBarrier.await(2, TimeUnit.SECONDS);

        assertThat(offeredElement.get()).isTrue();
    }

    /**
     * Concurrently performs put element until space becomes available in the
     * provided queue. This execution which will enable space is executed via
     * the provided {@link ExecutionInjection}.
     * 
     * @param executionToEnableSpace
     * @param queue
     * @throws Exception
     */
    private void concurrentPerformPutElementUntilSpaceIsAvailable(
            ExecutionInjection executionToEnableSpace,
            final BlockingQueue<Element> queue) throws Exception
    {
        final AtomicBoolean putElement = new AtomicBoolean(false);
        final CountDownLatch reachedLockBarrier = new CountDownLatch(1);
        final CountDownLatch putElementBarrier = new CountDownLatch(1);
        final ReentrantLock internalLock = extractInternalLockFrom(queue);
        final Element elementToOffer = new Element(QueuePriority.HIGH);
        myExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                internalLock.lock();
                try
                {
                    reachedLockBarrier.countDown();
                    queue.put(elementToOffer);
                    putElement.set(true);
                    putElementBarrier.countDown();
                } catch (InterruptedException e)
                {
                    // No point in doing anything...
                } finally
                {
                    internalLock.unlock();
                }
            }
        });

        reachedLockBarrier.await(10, TimeUnit.SECONDS);
        internalLock.lock();

        try
        {
            executionToEnableSpace.apply();
        } finally
        {
            internalLock.unlock();
        }

        putElementBarrier.await(2, TimeUnit.SECONDS);

        assertThat(putElement.get()).isTrue();
    }

    /**
     * Extracts the mutual exclusion lock from the provided queue.
     * 
     * @param queue
     * @return
     * @throws Exception
     */
    private ReentrantLock extractInternalLockFrom(BlockingQueue<Element> queue)
            throws Exception
    {
        Field field = queue.getClass().getDeclaredField("myAccessLock");
        field.setAccessible(true);
        return (ReentrantLock) field.get(queue);
    }

    private static int calculateSizeWhenQueuesInAllPrioritiesAreFull()
    {
        return QUEUE_CAPACITY * QueuePriority.values().length;
    }

    private List<Element> verifyAndAddAllElementsForAllPriorities(
            BlockingQueue<Element> queue, int numberOfElements)
    {
        List<Element> createdElements = new ArrayList<Element>();
        createdElements.addAll(verifyAndAddAllElements(queue, numberOfElements,
                QueuePriority.HIGH));
        createdElements.addAll(verifyAndAddAllElements(queue, numberOfElements,
                QueuePriority.MEDIUM));
        createdElements.addAll(verifyAndAddAllElements(queue, numberOfElements,
                QueuePriority.LOW));
        return createdElements;
    }

    private List<Element> verifyAndAddAllElements(BlockingQueue<Element> queue,
            int numberOfElements, QueuePriority queuePriority)
    {
        List<Element> createdElements = createElements(numberOfElements,
                queuePriority);

        for (Element e : createdElements)
        {
            boolean added = queue.add(e);

            if (!added)
            {
                fail("Should have inserted the specified element");
            }
        }

        return createdElements;
    }

    private List<Element> verifyAndOfferAllElementsForAllPriorities(
            BlockingQueue<Element> queue, int numberOfElements)
    {
        List<Element> createdElements = new ArrayList<Element>();
        createdElements.addAll(verifyAndOfferAllElements(queue,
                numberOfElements, QueuePriority.HIGH));
        createdElements.addAll(verifyAndOfferAllElements(queue,
                numberOfElements, QueuePriority.MEDIUM));
        createdElements.addAll(verifyAndOfferAllElements(queue,
                numberOfElements, QueuePriority.LOW));
        return createdElements;
    }

    private List<Element> verifyAndOfferAllElements(
            BlockingQueue<Element> queue, int numberOfElements,
            QueuePriority queuePriority)
    {
        List<Element> createdElements = createElements(numberOfElements,
                queuePriority);

        for (Element e : createdElements)
        {
            boolean added = queue.offer(e);

            if (!added)
            {
                fail("Should have inserted the specified element");
            }
        }

        return createdElements;
    }

    private List<Element> verifyAndOfferAllElementsForAllPriorities(
            BlockingQueue<Element> queue, int numberOfElements, long timeout,
            TimeUnit unit) throws InterruptedException
    {
        List<Element> createdElements = new ArrayList<Element>();
        createdElements.addAll(verifyAndOfferAllElements(queue,
                numberOfElements, QueuePriority.HIGH, timeout, unit));
        createdElements.addAll(verifyAndOfferAllElements(queue,
                numberOfElements, QueuePriority.MEDIUM, timeout, unit));
        createdElements.addAll(verifyAndOfferAllElements(queue,
                numberOfElements, QueuePriority.LOW, timeout, unit));
        return createdElements;
    }

    private List<Element> verifyAndOfferAllElements(
            BlockingQueue<Element> queue, int numberOfElements,
            QueuePriority queuePriority, long timeout, TimeUnit unit)
            throws InterruptedException
    {
        List<Element> createdElements = createElements(numberOfElements,
                queuePriority);

        for (Element e : createdElements)
        {
            boolean added = queue.offer(e, timeout, unit);

            if (!added)
            {
                fail("Should have inserted the specified element");
            }
        }

        return createdElements;
    }

    private List<Element> putAllElementsForAllPriorities(
            BlockingQueue<Element> queue, int numberOfElements)
            throws InterruptedException
    {
        List<Element> createdElements = new ArrayList<Element>();
        createdElements.addAll(putAllElements(queue, numberOfElements,
                QueuePriority.HIGH));
        createdElements.addAll(putAllElements(queue, numberOfElements,
                QueuePriority.MEDIUM));
        createdElements.addAll(putAllElements(queue, numberOfElements,
                QueuePriority.LOW));
        return createdElements;
    }

    private List<Element> putAllElements(BlockingQueue<Element> queue,
            int numberOfElements, QueuePriority queuePriority)
            throws InterruptedException
    {
        List<Element> createdElements = createElements(numberOfElements,
                queuePriority);

        for (Element e : createdElements)
        {
            queue.put(e);
        }

        return createdElements;
    }

    private List<Element> createElements(int numberOfElements,
            QueuePriority queuePriority)
    {
        List<Element> createdElements = new ArrayList<Element>(numberOfElements);

        for (int i = 0; i < numberOfElements; ++i)
        {
            createdElements.add(new Element(queuePriority));
        }

        return createdElements;
    }

    private BlockingQueue<Element> createDefaultStripedBlockingQueue()
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = createDefaultWeightPerPriority();
        return new StripedBlockingQueue<Element>(weightPerPriority,
                QUEUE_CAPACITY,
                calculateSizeWhenQueuesInAllPrioritiesAreFull(), false);
    }

    private EnumMap<QueuePriority, Integer> createDefaultWeightPerPriority()
    {
        EnumMap<QueuePriority, Integer> weightPerPriority = new EnumMap<QueuePriority, Integer>(
                QueuePriority.class);
        weightPerPriority.put(QueuePriority.HIGH, 60);
        weightPerPriority.put(QueuePriority.MEDIUM, 30);
        weightPerPriority.put(QueuePriority.LOW, 10);
        return weightPerPriority;
    }

    private static enum QueuePriority
    {
        HIGH, MEDIUM, LOW,
    }

    private static class Element implements StripedQueueElement
    {
        private final String myId;
        private final QueuePriority myQueuePriority;

        Element(QueuePriority queuePriority)
        {
            myId = queuePriority.toString();
            myQueuePriority = queuePriority;
        }

        Element(String id, QueuePriority queuePriority)
        {
            myId = id;
            myQueuePriority = queuePriority;
        }

        @Override
        public QueuePriority getElementPriority()
        {
            return myQueuePriority;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((myId == null) ? 0 : myId.hashCode());
            result = prime
                    * result
                    + ((myQueuePriority == null) ? 0 : myQueuePriority
                            .hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Element other = (Element) obj;
            if (myId == null)
            {
                if (other.myId != null)
                    return false;
            } else if (!myId.equals(other.myId))
                return false;
            if (myQueuePriority != other.myQueuePriority)
                return false;
            return true;
        }

        @Override
        public String toString()
        {
            return "" + myId;
        }
    }

    /**
     * A functional interface for injecting execution.
     * 
     */
    private static interface ExecutionInjection
    {
        /**
         * Applies the execution
         * 
         */
        public void apply();
    }
}
