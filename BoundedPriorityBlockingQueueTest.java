package gakesson.util.concurrent;

import static org.fest.assertions.Assertions.assertThat;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test class to verify the {@link BoundedPriorityBlockingQueue}.
 *
 * The private methods prefixed with "concurrent" is a bit verbose, but is so in order to get reliable
 * results without sleeping. Instead the verification is performed using signaling with locks
 * and barriers.
 * 
 */
public class BoundedPriorityBlockingQueueTest
{
    private static int ourElementCreationCounter = 0;

    private ExecutorService myExecutor;

    @BeforeMethod
    public void initTestCase()
    {
        ourElementCreationCounter = 0;
        myExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L,
                TimeUnit.SECONDS, new BoundedPriorityBlockingQueue<Runnable>(100));
    }

    @AfterMethod
    public void tearDownTestCase()
    {
        myExecutor.shutdownNow();
    }

    @Test (dataProvider = "elementsComparatorProvider", expectedExceptions = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionWhenAttemptingToUsingIteratorFromAnEmptyQueue(Comparator<Element> comparator)
    {
        int numberOfElements = 1;
        Iterator<Element> iterator = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator).iterator();
        iterator.remove();
    }

    @Test (dataProvider = "elementsComparatorProvider", expectedExceptions = NoSuchElementException.class)
    public void shouldThrowNoSuchElementExceptionWhenAttemptingToInvokeNextOnIteratorFromAnEmptyQueue(Comparator<Element> comparator)
    {
        int numberOfElements = 1;
        Iterator<Element> iterator = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator).iterator();
        iterator.next();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldCreatePriorityBlockingQueueUsingAnotherCollection(Comparator<Element> comparator)
    {
        int capacity = 200;
        int numberOfElementsToOffer = capacity;
        BlockingQueue<Element> anotherQueue = new PriorityBlockingQueue<Element>(capacity, comparator);
        offerAllElements(numberOfElementsToOffer, anotherQueue);

        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity, false, anotherQueue);

        assertThat(queue).hasSize(anotherQueue.size());
    }

    @Test
    public void shouldCreatePriorityBlockingQueueWithSpecifiedCapacity()
    {
        int capacity = 200;
        int numberOfElementsToOffer = capacity * 2;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity);
        offerAllElements(numberOfElementsToOffer, queue);

        assertThat(queue).hasSize(capacity);
    }

    @Test (dataProvider = "elementsComparatorProvider", expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenCapacityIsZero(Comparator<Element> comparator)
    {
        new BoundedPriorityBlockingQueue<Element>(0, comparator);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenInvokingDrainToUsingSameQueue()
    {
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(1);
        queue.drainTo(queue);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenInvokingDrainToUsingSameQueueAndUsingMaxElements()
    {
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(1);
        queue.drainTo(queue, 1);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldAddAllElements(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        verifyAndAddAllElements(numberOfElements, queue);
        
        assertThat(queue).hasSize(numberOfElements);
    }

    @Test (dataProvider = "elementsComparatorProvider", expectedExceptions = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionWhenAddingElementButViolatingCapacity(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        verifyAndAddAllElements(numberOfElements, queue);
        queue.add(new Element());
    }

    @Test (dataProvider = "elementsComparatorProvider", expectedExceptions = NullPointerException.class)
    public void shouldThrowNullPointerExceptionWhenAddingNullValue(Comparator<Element> comparator)
    {
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(1, comparator);
        queue.add(null);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldOfferAllElements(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        verifyAndOfferAllElements(numberOfElements, queue);

        assertThat(queue).hasSize(numberOfElements);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldSuccessfullyOfferAllElementsUsingASpecifiedTimeout(Comparator<Element> comparator) throws InterruptedException
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        verifyAndOfferAllElements(numberOfElements, queue, 1, TimeUnit.SECONDS);
        
        assertThat(queue).hasSize(numberOfElements);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldNotOfferElementWhenViolatingCapacity(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        verifyAndOfferAllElements(numberOfElements, queue);

        boolean inserted = queue.offer(new Element());

        assertThat(inserted).isFalse();
        assertThat(queue).hasSize(numberOfElements);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldNotOfferAllElementsWhenViolatingCapacityAndSpecifyingAZeroTimeout(Comparator<Element> comparator) throws InterruptedException
    {
        int capacity = 200;
        int numberOfElements = capacity * 2;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity, comparator);

        offerAllElements(numberOfElements, queue, 0, TimeUnit.SECONDS);
        
        assertThat(queue).hasSize(capacity);
    }

    @Test (dataProvider = "elementsComparatorProvider", expectedExceptions = NullPointerException.class)
    public void shouldThrowNullPointerExceptionWhenOfferNullValue(Comparator<Element> comparator)
    {
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(1, comparator);
        queue.offer(null);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPutAllElements(Comparator<Element> comparator) throws InterruptedException
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        putAllElements(numberOfElements, queue);
        
        assertThat(queue).hasSize(numberOfElements);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPollNullWhenQueueIsEmpty(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        Element element = queue.poll();
        
        assertThat(element).isNull();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPollAllFromQueue(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        int numberOfPolledElements = 0;

        while (!queue.isEmpty())
        {
            numberOfPolledElements++;
            Element element = queue.poll();
            assertThat(element).isNotNull();
        }

        assertThat(numberOfPolledElements).isEqualTo(numberOfElements);
        assertThat(queue).isEmpty();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldSuccessfullyPollAllElementsUsingASpecifiedTimeout(Comparator<Element> comparator) throws InterruptedException
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        int numberOfPolledElements = 0;

        while (!queue.isEmpty())
        {
            numberOfPolledElements++;
            Element element = queue.poll(1, TimeUnit.SECONDS);
            assertThat(element).isNotNull();
        }

        assertThat(numberOfPolledElements).isEqualTo(numberOfElements);
        assertThat(queue).isEmpty();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldNotPollMoreElementsThanAllowedWhenUsingAZeroTimeout(Comparator<Element> comparator) throws InterruptedException
    {
        int capacity = 100;
        int numberOfElements = capacity / 2;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        int numberOfPolledElements = 0;

        while (!queue.isEmpty())
        {
            numberOfPolledElements++;
            queue.poll();
        }

        Element element = queue.poll(0, TimeUnit.SECONDS);

        assertThat(element).isNull();
        assertThat(queue).isEmpty();
        assertThat(numberOfElements).isEqualTo(numberOfPolledElements);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldDrainQueue(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        BlockingQueue<Element> anotherQueue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        queue.drainTo(anotherQueue);

        assertThat(queue).isEmpty();
        assertThat(anotherQueue.size()).isEqualTo(numberOfElements);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldDrainQueueButOnlyToMaxElements(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        int maxElementsToDrain = numberOfElements / 2;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        BlockingQueue<Element> anotherQueue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        queue.drainTo(anotherQueue, maxElementsToDrain);

        assertThat(queue.size()).isEqualTo(maxElementsToDrain);
        assertThat(anotherQueue.size()).isEqualTo(maxElementsToDrain);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldNotDrainQueueWhenMaxElementsAreZero(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        int maxElementsToDrain = 0;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        BlockingQueue<Element> anotherQueue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        queue.drainTo(anotherQueue, maxElementsToDrain);
        
        assertThat(queue.size()).isEqualTo(numberOfElements);
        assertThat(anotherQueue).isEmpty();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldCopeWithDrainingEmptyQueueUsingMaxElements(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        int maxElementsToDrain = numberOfElements;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        BlockingQueue<Element> anotherQueue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        queue.drainTo(anotherQueue, maxElementsToDrain);

        assertThat(queue).isEmpty();
        assertThat(anotherQueue).isEmpty();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldCopeWithDrainingEmptyQueue(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        BlockingQueue<Element> anotherQueue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        queue.drainTo(anotherQueue);

        assertThat(queue).isEmpty();
        assertThat(anotherQueue).isEmpty();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldCopeWithDrainingQueueWhichSizeIsLessThanMaxElements(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        int maxElementsToDrain = numberOfElements * 2;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        BlockingQueue<Element> anotherQueue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        queue.drainTo(anotherQueue, maxElementsToDrain);

        assertThat(queue).isEmpty();
        assertThat(anotherQueue).hasSize(numberOfElements);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenThisQueueAndDrainToQueueAreSame()
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements);

        queue.drainTo(queue);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenThisQueueAndDrainToQueueAreSameAndUsingMaxElements()
    {
        int numberOfElements = 200;
        int maxElementsToDrain = numberOfElements / 2;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements);

        queue.drainTo(queue, maxElementsToDrain);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldIterateOverQueueElements(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        int numberOfIteratedElements = 0;

        for (Element element : queue)
        {
            numberOfIteratedElements++;

            if (!queue.contains(element))
            {
                fail("Queue should have contained element");
            }
        }

        assertThat(queue).hasSize(numberOfElements);
        assertThat(numberOfIteratedElements).isEqualTo(numberOfElements);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPeekAtElementWithHighestPriority(Comparator<Element> comparator)
    {
        Element elementWithHighestPriority = new Element(-1000);
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements((numberOfElements - 1), queue);
        queue.add(elementWithHighestPriority);

        Element element = queue.peek();

        assertThat(element).isSameAs(elementWithHighestPriority);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldReturnNullWhenPeekingEmptyQueue(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        Element element = queue.peek();

        assertThat(element).isNull();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldCalculateRemainingCapacity(Comparator<Element> comparator)
    {
        int capacity = 200;
        int numberOfElements = capacity / 3;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);

        int expectedRemainingCapacity = capacity - numberOfElements;
        
        assertThat(queue.remainingCapacity()).isEqualTo(expectedRemainingCapacity);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldReturnCurrentSizeOfQueue(Comparator<Element> comparator)
    {
        int capacity = 200;
        int numberOfElements = 200 / 2;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);

        assertThat(queue).hasSize(numberOfElements);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldReturnSizeZeroWhenQueueIsEmpty(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);

        assertThat(queue).isEmpty();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldRemoveElementsByEquality(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        Iterator<Element> createdElements = verifyAndOfferAllElements(numberOfElements, queue).iterator();
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
        assertThat(numberOfElements).isEqualTo(numberOfRemovals);
        assertThat(createdElements.hasNext()).isFalse();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldNotRemoveNonExistingElement(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);

        boolean removed = queue.remove(new Element());

        assertThat(removed).isFalse();
        assertThat(queue).hasSize(numberOfElements);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldReturnArrayRepresentationOfQueue(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        List<Element> createdElements = verifyAndOfferAllElements(numberOfElements, queue);

        Element[] elements = queue.toArray(new Element[queue.size()]);

        assertThat(createdElements).hasSize(elements.length);
        assertThat(createdElements.containsAll(Arrays.asList(elements))).isTrue();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldReturnArrayRepresentationOfQueueWhenProvidingArrayWhichIsGreaterThanQueueSize(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        int newArraySize = queue.size() * 2;

        Element[] elements = queue.toArray(new Element[newArraySize]);

        assertThat(elements).hasSize(newArraySize);
        assertThat(elements[queue.size()]).isNull();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldReturnArrayRepresentationOfQueueUsingNoArguments(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        List<Element> createdElements = verifyAndOfferAllElements(numberOfElements, queue);

        Object[] elements = queue.toArray();

        assertThat(createdElements).hasSize(elements.length);
        assertThat(createdElements.containsAll(Arrays.asList(elements))).isTrue();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldReturnArrayRepresentationOfQueueByProvidingSizeLessThanQueueSize(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        List<Element> createdElements = verifyAndOfferAllElements(numberOfElements, queue);

        Element[] elements = queue.toArray(new Element[queue.size() / 2]);

        assertThat(createdElements).hasSize(elements.length);
        assertThat(createdElements.containsAll(Arrays.asList(elements))).isTrue();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldClearElementsInQueue(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);

        queue.clear();
        
        assertThat(queue).isEmpty();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldContainElement(Comparator<Element> comparator)
    {
        int capacity = 200;
        int numberOfElements = 100;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        Element element = new Element();
        queue.add(element);

        boolean exists = queue.contains(element);

        assertThat(exists).isTrue();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldNotContainNull(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);

        boolean exists = queue.contains(null);

        assertThat(exists).isFalse();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldNotBeAbleToRemoveNull(Comparator<Element> comparator)
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);

        boolean removed = queue.remove(null);

        assertThat(removed).isFalse();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldTakeAllFromQueue(Comparator<Element> comparator) throws InterruptedException
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);

        int numberOfPolledElements = 0;

        while (!queue.isEmpty())
        {
            numberOfPolledElements++;
            Element element = queue.take();
            assertThat(element).isNotNull();
        }

        assertThat(numberOfPolledElements).isEqualTo(numberOfElements);
        assertThat(queue).isEmpty();
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPrioritizeElementsWithRandomSequenceNumbers(Comparator<Element> comparator) throws InterruptedException
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        List<Element> createdElements = createElements(numberOfElements, new Random(System.nanoTime()));

        for (Element e : createdElements)
        {
            queue.offer(e);
        }

        Collections.sort(createdElements);
        List<Element> sortedElementsPolledFromQueue = new ArrayList<Element>();

        while (!queue.isEmpty())
        {
            Element element = queue.poll();
            sortedElementsPolledFromQueue.add(element);
        }

        assertThat(sortedElementsPolledFromQueue).hasSize(createdElements.size());
        assertThat(sortedElementsPolledFromQueue).isEqualTo(createdElements);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldKeepPrioritizonOrderAfterRandomlyRemovingHalfOfTheElements(Comparator<Element> comparator) throws InterruptedException
    {
        int numberOfElements = 200;
        int numberOfElementsToRemove = numberOfElements / 2;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        List<Element> createdElements = createElements(numberOfElements, new Random(System.nanoTime()));

        for (Element e : createdElements)
        {
            queue.offer(e);
        }

        while (queue.size() > numberOfElementsToRemove)
        {
            int indexOfElementToRemove = (int) (System.nanoTime() % createdElements.size());
            Element elementToRemove = createdElements.get(indexOfElementToRemove);
            queue.remove(elementToRemove);
            createdElements.remove(indexOfElementToRemove);
        }

        Collections.sort(createdElements);
        List<Element> sortedElements = new ArrayList<Element>();

        while (!queue.isEmpty())
        {
            Element element = queue.poll();
            sortedElements.add(element);
        }

        assertThat(sortedElements).hasSize(createdElements.size());
        assertThat(sortedElements).isEqualTo(createdElements);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldKeepPrioritizonOrderWhenDrainingQueue(Comparator<Element> comparator) throws InterruptedException
    {
        int numberOfElements = 200;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        BlockingQueue<Element> originalQueue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, false, queue);
        List<Element> collectionToDrainTo = new ArrayList<Element>();
        queue.drainTo(collectionToDrainTo);
        int counter = 0;

        while (!originalQueue.isEmpty())
        {
            Element originalElement = originalQueue.poll();
            Element drainedElement = collectionToDrainTo.get(counter++);

            if (originalElement != drainedElement)
            {
                fail("Should have kept the prioritization order while draining");
            }
        }

        assertThat(counter).isEqualTo(numberOfElements);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldKeepPrioritizonOrderWhenDrainingQueueUsingMaxElements(Comparator<Element> comparator) throws InterruptedException
    {
        int numberOfElements = 200;
        int maxElements = numberOfElements;
        BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        BlockingQueue<Element> originalQueue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, false, queue);
        List<Element> collectionToDrainTo = new ArrayList<Element>();
        queue.drainTo(collectionToDrainTo, maxElements);
        int counter = 0;

        while (!originalQueue.isEmpty())
        {
            Element originalElement = originalQueue.poll();
            Element drainedElement = collectionToDrainTo.get(counter++);

            if (originalElement != drainedElement)
            {
                fail("Should have kept the prioritization order while draining");
            }
        }

        assertThat(counter).isEqualTo(numberOfElements);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldOfferElementToFullQueueUntilQueueIsCleared(Comparator<Element> comparator) throws Exception
    {
        int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.clear();
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurentPerformOfferElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldOfferElementToFullQueueUntilQueueIsDrained(final Comparator<Element> comparator) throws Exception
    {
        final int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.drainTo(new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator));
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurentPerformOfferElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldOfferElementToFullQueueUntilQueueIsDrainedUsingMaxElements(final Comparator<Element> comparator) throws Exception
    {
        final int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                int maxElements = numberOfElements * 2;
                queue.drainTo(new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator), maxElements);
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurentPerformOfferElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldOfferElementToFullQueueUntilAnotherElementIsRemoved(Comparator<Element> comparator) throws Exception
    {
        int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        final Element firstElement = verifyAndOfferAllElements(numberOfElements, queue).iterator().next();
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.remove(firstElement);
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurentPerformOfferElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldOfferElementToFullQueueUntilAnotherElementIsRemovedUsingIterator(Comparator<Element> comparator) throws Exception
    {
        final int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
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

        assertThat(queue.remainingCapacity()).isZero();

        concurentPerformOfferElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldOfferElementToFullQueueUntilAnotherElementIsPolled(Comparator<Element> comparator) throws Exception
    {
        final int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.poll();
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurentPerformOfferElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldOfferElementToFullQueueUntilAnotherElementIsPolledUsingSpecifiedTimeout(Comparator<Element> comparator) throws Exception
    {
        final int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.poll(10, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurentPerformOfferElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldOfferElementToFullQueueUntilAnotherElementIsTaken(Comparator<Element> comparator) throws Exception
    {
        final int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.take();
                }
                catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurentPerformOfferElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPutElementToFullQueueUntilQueueIsCleared(Comparator<Element> comparator) throws Exception
    {
        int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.clear();
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurrentPerformPutElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPutElementToFullQueueUntilQueueIsDrained(final Comparator<Element> comparator) throws Exception
    {
        final int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.drainTo(new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator));
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurrentPerformPutElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPutElementToFullQueueUntilQueueIsDrainedUsingMaxElements(final Comparator<Element> comparator) throws Exception
    {
        final int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                int maxElements = numberOfElements * 2;
                queue.drainTo(new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator), maxElements);
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurrentPerformPutElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPutElementToFullQueueUntilAnotherElementIsRemoved(Comparator<Element> comparator) throws Exception
    {
        int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        final Element firstElement = verifyAndOfferAllElements(numberOfElements, queue).iterator().next();
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.remove(firstElement);
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurrentPerformPutElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPutElementToFullQueueUntilAnotherElementIsRemovedUsingIterator(Comparator<Element> comparator) throws Exception
    {
        final int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
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

        assertThat(queue.remainingCapacity()).isZero();

        concurrentPerformPutElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPutElementToFullQueueUntilAnotherElementIsPolled(Comparator<Element> comparator) throws Exception
    {
        final int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.poll();
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurrentPerformPutElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPutElementToFullQueueUntilAnotherElementIsPolledUsingSpecifiedTimeout(Comparator<Element> comparator) throws Exception
    {
        final int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.poll(10, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurrentPerformPutElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPutElementToFullQueueUntilAnotherElementIsTaken(Comparator<Element> comparator) throws Exception
    {
        final int numberOfElements = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(numberOfElements, comparator);
        verifyAndOfferAllElements(numberOfElements, queue);
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.take();
                }
                catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue.remainingCapacity()).isZero();

        concurrentPerformPutElementUntilSpaceIsAvailable(executionToEnableSpace, queue);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPollElementFromEmptyQueueUntilElementIsOffered(Comparator<Element> comparator) throws Exception
    {
        final int capacity = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity, comparator);
        final Element elementToAdd = new Element();
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.offer(elementToAdd);
            }
        };

        assertThat(queue.remainingCapacity()).isEqualTo(capacity);

        concurrentPerformPollUntilElementIsAvailable(executionToEnableSpace, queue, elementToAdd);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPollElementFromEmptyQueueUntilElementIsOfferedUsingSpecifiedTimeout(Comparator<Element> comparator) throws Exception
    {
        final int capacity = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity, comparator);
        final Element elementToAdd = new Element();
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.offer(elementToAdd, 10, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue.remainingCapacity()).isEqualTo(capacity);

        concurrentPerformPollUntilElementIsAvailable(executionToEnableSpace, queue, elementToAdd);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPollElementFromEmptyQueueUntilElementIsPut(Comparator<Element> comparator) throws Exception
    {
        final int capacity = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity, comparator);
        final Element elementToAdd = new Element();
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.put(elementToAdd);
                }
                catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue.remainingCapacity()).isEqualTo(capacity);

        concurrentPerformPollUntilElementIsAvailable(executionToEnableSpace, queue, elementToAdd);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldPollElementFromEmptyQueueUntilElementIsAdded(Comparator<Element> comparator) throws Exception
    {
        final int capacity = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity, comparator);
        final Element elementToAdd = new Element();
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.add(elementToAdd);
            }
        };

        assertThat(queue.remainingCapacity()).isEqualTo(capacity);

        concurrentPerformPollUntilElementIsAvailable(executionToEnableSpace, queue, elementToAdd);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldTakeElementFromEmptyQueueUntilElementIsOffered(Comparator<Element> comparator) throws Exception
    {
        final int capacity = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity, comparator);
        final Element elementToAdd = new Element();
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.offer(elementToAdd);
            }
        };

        assertThat(queue.remainingCapacity()).isEqualTo(capacity);

        concurrentPerformTakeUntilElementIsAvailable(executionToEnableSpace, queue, elementToAdd);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldTakeElementFromEmptyQueueUntilElementIsOfferedUsingSpecifiedTimeout(Comparator<Element> comparator) throws Exception
    {
        final int capacity = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity, comparator);
        final Element elementToAdd = new Element();
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.offer(elementToAdd, 10, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue.remainingCapacity()).isEqualTo(capacity);

        concurrentPerformTakeUntilElementIsAvailable(executionToEnableSpace, queue, elementToAdd);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldTakeElementFromEmptyQueueUntilElementIsPut(Comparator<Element> comparator) throws Exception
    {
        final int capacity = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity, comparator);
        final Element elementToAdd = new Element();
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                try
                {
                    queue.put(elementToAdd);
                }
                catch (InterruptedException e)
                {
                    // Nothing we can do...
                }
            }
        };

        assertThat(queue.remainingCapacity()).isEqualTo(capacity);

        concurrentPerformTakeUntilElementIsAvailable(executionToEnableSpace, queue, elementToAdd);
    }

    @Test (dataProvider = "elementsComparatorProvider")
    public void shouldTakeElementFromEmptyQueueUntilElementIsAdded(Comparator<Element> comparator) throws Exception
    {
        final int capacity = 200;
        final BlockingQueue<Element> queue = new BoundedPriorityBlockingQueue<Element>(capacity, comparator);
        final Element elementToAdd = new Element();
        ExecutionInjection executionToEnableSpace = new ExecutionInjection()
        {
            @Override
            public void apply()
            {
                queue.add(elementToAdd);
            }
        };

        assertThat(queue.remainingCapacity()).isEqualTo(capacity);

        concurrentPerformTakeUntilElementIsAvailable(executionToEnableSpace, queue, elementToAdd);
    }

    /**
     * Concurrently performs take element until space becomes available in the provided queue. This execution which will enable space is executed via
     * the provided {@link ExecutionInjection}.
     * 
     * @param executionToAddElementToQueue
     * @param queue
     * @param elementToBeAdded
     * @throws Exception
     */
    private void concurrentPerformTakeUntilElementIsAvailable(ExecutionInjection executionToAddElementToQueue, final BlockingQueue<Element> queue, Element elementToBeAdded) throws Exception
    {
        final AtomicReference<Element> takenElement = new AtomicReference<Element>();
        final CountDownLatch reachedLockBarrier = new CountDownLatch(1);
        final CountDownLatch takenElementBarrier = new CountDownLatch(1);
        final ReentrantLock internalLock = extractInternalLockFrom(queue);
        myExecutor.execute(new ComparableRunnable()
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
                }
                catch (InterruptedException e)
                {
                    // No point in doing anything...
                }
                finally
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
        }
        finally
        {
            internalLock.unlock();
        }

        takenElementBarrier.await(2, TimeUnit.SECONDS);

        assertThat(takenElement.get()).isSameAs(elementToBeAdded);
    }

    /**
     * Concurrently performs poll element until space becomes available in the provided queue. This execution which will enable space is executed via
     * the provided {@link ExecutionInjection}.
     * 
     * @param executionToAddElementToQueue
     * @param queue
     * @param elementToBeAdded
     * @throws Exception
     */
    private void concurrentPerformPollUntilElementIsAvailable(ExecutionInjection executionToAddElementToQueue, final BlockingQueue<Element> queue, Element elementToBeAdded) throws Exception
    {
        final AtomicReference<Element> polledElement = new AtomicReference<Element>();
        final CountDownLatch reachedLockBarrier = new CountDownLatch(1);
        final CountDownLatch polledElementBarrier = new CountDownLatch(1);
        final ReentrantLock internalLock = extractInternalLockFrom(queue);
        myExecutor.execute(new ComparableRunnable()
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
                }
                catch (InterruptedException e)
                {
                    // No point in doing anything...
                }
                finally
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
        }
        finally
        {
            internalLock.unlock();
        }

        polledElementBarrier.await(2, TimeUnit.SECONDS);

        assertThat(polledElement.get()).isSameAs(elementToBeAdded);
    }

    /**
     * Concurrently performs offer element until space becomes available in the provided queue. This execution which will enable space is executed via
     * the provided {@link ExecutionInjection}.
     * 
     * @param executionToEnableSpace
     * @param queue
     * @throws Exception
     */
    private void concurentPerformOfferElementUntilSpaceIsAvailable(ExecutionInjection executionToEnableSpace, final BlockingQueue<Element> queue) throws Exception
    {
        final AtomicBoolean offeredElement = new AtomicBoolean(false);
        final CountDownLatch reachedLockBarrier = new CountDownLatch(1);
        final CountDownLatch offeredElementBarrier = new CountDownLatch(1);
        final ReentrantLock internalLock = extractInternalLockFrom(queue);
        final Element elementToOffer = new Element();
        myExecutor.execute(new ComparableRunnable()
        {
            @Override
            public void run()
            {
                internalLock.lock();
                try
                {
                    reachedLockBarrier.countDown();
                    boolean isInserted = queue.offer(elementToOffer, 30, TimeUnit.SECONDS);
                    offeredElement.set(isInserted);
                    offeredElementBarrier.countDown();
                }
                catch (InterruptedException e)
                {
                    // No point in doing anything...
                }
                finally
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
        }
        finally
        {
            internalLock.unlock();
        }

        offeredElementBarrier.await(2, TimeUnit.SECONDS);

        assertThat(offeredElement.get()).isTrue();
    }

    /**
     * Concurrently performs put element until space becomes available in the provided queue. This execution which will enable space is executed via
     * the provided {@link ExecutionInjection}.
     * 
     * @param executionToEnableSpace
     * @param queue
     * @throws Exception
     */
    private void concurrentPerformPutElementUntilSpaceIsAvailable(ExecutionInjection executionToEnableSpace, final BlockingQueue<Element> queue) throws Exception
    {
        final AtomicBoolean putElement = new AtomicBoolean(false);
        final CountDownLatch reachedLockBarrier = new CountDownLatch(1);
        final CountDownLatch putElementBarrier = new CountDownLatch(1);
        final ReentrantLock internalLock = extractInternalLockFrom(queue);
        final Element elementToOffer = new Element();
        myExecutor.execute(new ComparableRunnable()
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
                }
                catch (InterruptedException e)
                {
                    // No point in doing anything...
                }
                finally
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
        }
        finally
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
    private ReentrantLock extractInternalLockFrom(BlockingQueue<Element> queue) throws Exception
    {
        Field field = queue.getClass().getDeclaredField("myLock");
        field.setAccessible(true);
        return (ReentrantLock) field.get(queue);
    }

    /**
     * Offer X number of {@link Element}s into the queue, using a specified timeout. This method performs verification whether or not the offer
     * operation succeeded.
     * 
     * @param numberOfElements
     * @param queue
     * @return
     */
    private List<Element> verifyAndOfferAllElements(int numberOfElements, BlockingQueue<Element> queue)
    {
        List<Element> createdElements = createElements(numberOfElements);

        for (Element e : createdElements)
        {
            boolean inserted = queue.offer(e);

            if (!inserted)
            {
                fail("Should have offered the specified element");
            }
        }

        return createdElements;
    }

    /**
     * Offer X number of {@link Element}s into the queue, using a specified timeout. This method performs verification whether or not the offer
     * operation succeeded.
     * 
     * @param numberOfElements
     * @param queue
     * @param timeout
     * @param timeUnit
     * @return
     * @throws InterruptedException
     */
    private List<Element> verifyAndOfferAllElements(int numberOfElements, BlockingQueue<Element> queue, long timeout, TimeUnit timeUnit) throws InterruptedException
    {
        List<Element> createdElements = createElements(numberOfElements);

        for (Element e : createdElements)
        {
            boolean inserted = queue.offer(e, timeout, timeUnit);

            if (!inserted)
            {
                fail("Should have offered the specified element");
            }
        }

        return createdElements;
    }

    /**
     * Offer X number of {@link Element}s into the queue, using a specified timeout. This method performs no verification whether or not the offer
     * operation succeeded.
     * 
     * @param numberOfElements
     * @param queue
     * @param timeout
     * @param timeUnit
     * @return
     * @throws InterruptedException
     */
    private List<Element> offerAllElements(int numberOfElements, BlockingQueue<Element> queue, long timeout, TimeUnit timeUnit) throws InterruptedException
    {
        List<Element> createdElements = createElements(numberOfElements);

        for (Element e : createdElements)
        {
            queue.offer(e, timeout, timeUnit);
        }

        return createdElements;
    }

    /**
     * Adds X number of {@link Element}s into the queue. This method performs verification whether or not the add operation succeeded.
     * 
     * @param numberOfElements
     * @param queue
     * @return
     */
    private List<Element> verifyAndAddAllElements(int numberOfElements, BlockingQueue<Element> queue)
    {
        List<Element> createdElements = createElements(numberOfElements);

        for (Element e : createdElements)
        {
            boolean added = queue.add(e);

            if (!added)
            {
                fail("Should have added the specified element");
            }
        }

        return createdElements;
    }

    /**
     * Offer X number of {@link Element}s into the queue. In case X is greater than queue capacity, this method will block.
     * 
     * @param numberOfElements
     * @param queue
     * @return
     * @throws InterruptedException
     */
    private List<Element> putAllElements(int numberOfElements, BlockingQueue<Element> queue) throws InterruptedException
    {
        List<Element> createdElements = createElements(numberOfElements);

        for (Element e : createdElements)
        {
            queue.put(e);
        }

        return createdElements;
    }

    /**
     * Offer X number of {@link Element}s into the queue. This method performs no verification whether or not the offer operation succeeded.
     * 
     * @param numberOfElements
     * @param queue
     * @return
     */
    private List<Element> offerAllElements(int numberOfElements, BlockingQueue<Element> queue)
    {
        List<Element> createdElements = createElements(numberOfElements);

        for (Element e : createdElements)
        {
            queue.offer(e);
        }

        return createdElements;
    }

    /**
     * Creates X number of {@link Element}s with generated identity numbers.
     * 
     * @param numberOfElements
     * @return
     */
    private List<Element> createElements(int numberOfElements)
    {
        return createElements(numberOfElements, null);
    }

    /**
     * Creates X number of {@link Element}s. In case a {@link Random} is provided, the identity numbers will be randomly generated, whereas if not
     * provided a number will be generated and assigned.
     * 
     * @param numberOfElements
     * @param randomIdentityGenerator
     * @return
     */
    private List<Element> createElements(int numberOfElements, Random randomIdentityGenerator)
    {
        List<Element> createdElements = new ArrayList<Element>(numberOfElements);

        for (int i = 0; i < numberOfElements; ++i)
        {
            if (randomIdentityGenerator != null)
            {
                int randomIdentityNumber = Math.abs(randomIdentityGenerator.nextInt());
                createdElements.add(new Element(randomIdentityNumber));
            }
            else
            {
                createdElements.add(new Element());
            }
        }

        Collections.shuffle(createdElements);
        return createdElements;
    }

    /**
     * An implementation representing the elements to store in a {@link BoundedPriorityBlockingQueue}.
     * 
     */
    private final class Element implements Comparable<Element>
    {
        private final int myIdentity;

        /**
         * Creates a new {@link Element} with a generated identity number.
         * 
         */
        Element()
        {
            myIdentity = ++ourElementCreationCounter;
        }

        /**
         * Creates a new {@link Element} with the provided identity number.
         * 
         * @param identityNumber
         */
        Element(int identityNumber)
        {
            myIdentity = identityNumber;
        }

        @Override
        public int compareTo(Element o)
        {
            return myIdentity - o.myIdentity;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + myIdentity;
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            Element other = (Element) obj;
            if (myIdentity != other.myIdentity) return false;
            return true;
        }

        @Override
        public String toString()
        {
            return "Element with identity number " + myIdentity;
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

    /**
     * An implementation which is both {@link Runnable} and {@link Comparable}. Used for semi-test
     * of the queue in a thread pool.
     * 
     */
    private static abstract class ComparableRunnable implements Runnable, Comparable<ExecutionInjection>
    {
        @Override
        public abstract void run();

        @Override
        public int compareTo(ExecutionInjection other)
        {
        	// Don't care...
            return 0;
        }
    }

    /**
     * Creates a {@link Comparator} for {@link Element}s.
     * 
     * @return
     */
    private Comparator<Element> createElementsComparator()
    {
        return new Comparator<Element>()
        {
            @Override
            public int compare(Element o1, Element o2)
            {
                return o1.myIdentity - o2.myIdentity;
            }
        };
    }

    /**
     * A data provider to either provide a {@link Comparator} or not. The queue will behave differently depending
     * on this input.
     * 
     * @return
     */
    @DataProvider
    public Object[][] elementsComparatorProvider()
    {
        final Comparator<Element> noComparator = null;
        final Comparator<Element> comparator = createElementsComparator();
        return new Object[][]
        {
                { comparator },
                { noComparator } };
    }
}
