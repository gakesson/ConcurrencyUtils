package gakesson.util.concurrent;

import static org.fest.assertions.Assertions.assertThat;
import static org.testng.Assert.fail;

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import gakesson.util.concurrent.ProducerThrottlingQueueElement.ThrottlingPolicy;

/**
 * Test class to verify the {@link ProducerThrottlingBlockingQueue}. The tests
 * are carried out using a {@link LinkedBlockingQueue} as the backing queue.
 * 
 * The private methods prefixed with "concurrent" are a bit verbose, but is so
 * in order to get reliable results without sleeping. Instead the verification
 * is performed using signaling with locks and barriers.
 * 
 */
public class ProducerThrottlingBlockingQueueTest {
	private static final String PRODUCER_IDENTIFIER_1 = "ProducerIdentifier1";
	private static final String PRODUCER_IDENTIFIER_2 = "ProducerIdentifier2";
	private static final String PRODUCER_IDENTIFIER_3 = "ProducerIdentifier3";
	private static final String DEFAULT_PRODUCER_IDENTIFIER = PRODUCER_IDENTIFIER_1;

	private static final int PRODUCER_CAPACITY = 100;

	private ExecutorService myExecutor;

	@BeforeMethod
	public void initTestCase() {
		myExecutor = Executors.newCachedThreadPool();
	}

	@AfterMethod
	public void tearDownTestCase() throws Exception {
		myExecutor.shutdownNow();
	}

	@Test
	public void shouldAddAllElements() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();

		verifyAndAddAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test
	public void shouldCopeWhenBackingQueueIsRejectingOfferOfElement() {
		BlockingQueue<Element> queue = new ProducerThrottlingBlockingQueue<Element>(
				new LinkedBlockingQueue<Element>(1));
		Element element = createDefaultElement();

		queue.offer(element);
		queue.offer(element);

		assertThat(queue).hasSize(1);

		queue.poll();

		assertThat(queue).isEmpty();

		queue.offer(element);

		assertThat(queue).hasSize(1);
	}

	@Test
	public void shouldCopeWhenBackingQueueIsRejectingOfferOfElementUsingSpecifiedTimeout()
			throws Exception {
		BlockingQueue<Element> queue = new ProducerThrottlingBlockingQueue<Element>(
				new LinkedBlockingQueue<Element>(1));
		Element element = createDefaultElement();

		queue.offer(element, 0, TimeUnit.SECONDS);
		queue.offer(element, 0, TimeUnit.SECONDS);

		assertThat(queue).hasSize(1);

		queue.poll();

		assertThat(queue).isEmpty();

		queue.offer(element, 0, TimeUnit.SECONDS);

		assertThat(queue).hasSize(1);
	}

	@Test
	public void shouldOfferElementWithAttemptThrottleEvenThoughQueueShouldThrottle() {
		int maximumProducerCapacity = 1;
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		Element attemptThrottleElement = new Element(
				DEFAULT_PRODUCER_IDENTIFIER, maximumProducerCapacity,
				ThrottlingPolicy.ATTEMPT_THROTTLE);
		Element mustThrottleElement = new Element(DEFAULT_PRODUCER_IDENTIFIER,
				maximumProducerCapacity, ThrottlingPolicy.MUST_THROTTLE);

		queue.offer(attemptThrottleElement);
		boolean attemptThrottleElementInserted = queue
				.offer(attemptThrottleElement);
		boolean mustThrottleElementInserted = queue.offer(mustThrottleElement);

		assertThat(attemptThrottleElementInserted).isTrue();
		assertThat(mustThrottleElementInserted).isFalse();

		assertThat(queue.poll()).isSameAs(attemptThrottleElement);
		assertThat(queue.poll()).isSameAs(attemptThrottleElement);

		mustThrottleElementInserted = queue.offer(mustThrottleElement);

		assertThat(mustThrottleElementInserted).isTrue();

		assertThat(queue.poll()).isSameAs(mustThrottleElement);
	}

	@Test
	public void shouldOfferElementWithSpecifiedTimeoutAndWithAttemptThrottleEvenThoughQueueShouldThrottle()
			throws Exception {
		int maximumProducerCapacity = 1;
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		Element attemptThrottleElement = new Element(
				DEFAULT_PRODUCER_IDENTIFIER, maximumProducerCapacity,
				ThrottlingPolicy.ATTEMPT_THROTTLE);
		Element mustThrottleElement = new Element(DEFAULT_PRODUCER_IDENTIFIER,
				maximumProducerCapacity, ThrottlingPolicy.MUST_THROTTLE);

		queue.offer(attemptThrottleElement, 0, TimeUnit.SECONDS);
		boolean attemptThrottleElementInserted = queue.offer(
				attemptThrottleElement, 0, TimeUnit.SECONDS);
		boolean mustThrottleElementInserted = queue.offer(mustThrottleElement,
				0, TimeUnit.SECONDS);

		assertThat(attemptThrottleElementInserted).isTrue();
		assertThat(mustThrottleElementInserted).isFalse();

		assertThat(queue.poll()).isSameAs(attemptThrottleElement);
		assertThat(queue.poll()).isSameAs(attemptThrottleElement);

		mustThrottleElementInserted = queue.offer(mustThrottleElement, 0,
				TimeUnit.SECONDS);

		assertThat(mustThrottleElementInserted).isTrue();

		assertThat(queue.poll()).isSameAs(mustThrottleElement);
	}

	@Test
	public void shouldPutElementWithAttemptThrottleEvenThoughQueueShouldThrottle()
			throws Exception {
		int maximumProducerCapacity = 1;
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		Element attemptThrottleElement = new Element(
				DEFAULT_PRODUCER_IDENTIFIER, maximumProducerCapacity,
				ThrottlingPolicy.ATTEMPT_THROTTLE);
		Element mustThrottleElement = new Element(DEFAULT_PRODUCER_IDENTIFIER,
				maximumProducerCapacity, ThrottlingPolicy.MUST_THROTTLE);

		queue.put(attemptThrottleElement);
		queue.put(attemptThrottleElement);

		assertThat(queue.poll()).isSameAs(attemptThrottleElement);
		assertThat(queue.poll()).isSameAs(attemptThrottleElement);

		queue.put(mustThrottleElement);

		assertThat(queue.poll()).isSameAs(mustThrottleElement);
	}

	@Test
	public void shouldAddElementWithAttemptThrottleEvenThoughQueueShouldThrottle()
			throws Exception {
		int maximumProducerCapacity = 1;
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		Element attemptThrottleElement = new Element(
				DEFAULT_PRODUCER_IDENTIFIER, maximumProducerCapacity,
				ThrottlingPolicy.ATTEMPT_THROTTLE);
		Element mustThrottleElement = new Element(DEFAULT_PRODUCER_IDENTIFIER,
				maximumProducerCapacity, ThrottlingPolicy.MUST_THROTTLE);

		queue.add(attemptThrottleElement);
		boolean attemptThrottleElementInserted = queue
				.add(attemptThrottleElement);

		assertThat(attemptThrottleElementInserted).isTrue();

		try {
			queue.add(mustThrottleElement);
			fail("Should have failed");
		} catch (IllegalStateException e) {
			// Expected
		}

		assertThat(queue.poll()).isSameAs(attemptThrottleElement);
		assertThat(queue.poll()).isSameAs(attemptThrottleElement);

		boolean mustThrottleElementInserted = queue.add(mustThrottleElement);

		assertThat(mustThrottleElementInserted).isTrue();
		assertThat(queue.poll()).isSameAs(mustThrottleElement);
	}

	@Test
	public void shouldOfferAllElements() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();

		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test
	public void shouldSuccessfullyOfferAllElementsUsingASpecifiedTimeout()
			throws InterruptedException {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();

		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY, 10,
				TimeUnit.SECONDS);

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test
	public void shouldSuccessfullyPutAllElementsUsingASpecifiedTimeout()
			throws InterruptedException {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();

		putAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test
	public void shouldNotOfferElementWhenViolatingProducerCapacity() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();

		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		boolean inserted = queue.offer(createDefaultElement());

		assertThat(inserted).isFalse();
		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test
	public void shouldNotOfferElementWhenViolatingProducerCapacityAndSpecifyingAZeroTimeout()
			throws InterruptedException {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();

		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		boolean inserted = queue.offer(createDefaultElement(), 0,
				TimeUnit.SECONDS);

		assertThat(inserted).isFalse();
		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void shouldThrowNullPointerExceptionWhenOfferNullValue() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		queue.offer(null);
	}

	@Test
	public void shouldPollNullWhenQueueIsEmpty() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();

		Element element = queue.poll();

		assertThat(element).isNull();
	}

	@Test
	public void shouldPollAllFromQueue() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		int numberOfPolledElements = 0;

		while (!queue.isEmpty()) {
			numberOfPolledElements++;
			Element element = queue.poll();
			assertThat(element).isNotNull();
		}

		assertThat(numberOfPolledElements).isEqualTo(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
		assertThat(queue).isEmpty();
	}

	@Test
	public void shouldSuccessfullyPollAllUsingASpecifiedTimeout()
			throws InterruptedException {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		int numberOfPolledElements = 0;

		while (!queue.isEmpty()) {
			numberOfPolledElements++;
			Element element = queue.poll(1, TimeUnit.SECONDS);
			assertThat(element).isNotNull();
		}

		assertThat(numberOfPolledElements).isEqualTo(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
		assertThat(queue).isEmpty();
	}

	@Test
	public void shouldBeAbleToOfferElementsAfterQueueIsDepleted() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		queue.clear();

		assertThat(queue).isEmpty();

		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test
	public void shouldBeAbleToOfferElementsWithASpecifiedTimeoutAfterQueueIsDepleted()
			throws Exception {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY, 10,
				TimeUnit.SECONDS);

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		queue.clear();

		assertThat(queue).isEmpty();

		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY, 10,
				TimeUnit.SECONDS);

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test
	public void shouldBeAbleToPutElementsAfterQueueIsDepleted()
			throws Exception {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		putAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		queue.clear();

		assertThat(queue).isEmpty();

		putAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test
	public void shouldBeAbleToAddElementsAfterQueueIsDepleted()
			throws Exception {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndAddAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		queue.clear();

		assertThat(queue).isEmpty();

		verifyAndAddAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test
	public void shouldNotPollMoreElementsThanAllowedWhenUsingAZeroTimeout()
			throws InterruptedException {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		int numberOfPolledElements = 0;

		while (!queue.isEmpty()) {
			numberOfPolledElements++;
			queue.poll();
		}

		Element element = queue.poll(0, TimeUnit.SECONDS);

		assertThat(element).isNull();
		assertThat(numberOfPolledElements).isEqualTo(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
		assertThat(queue).isEmpty();
	}

	@Test
	public void shouldBeAbleToOfferElementAfterDrainingFullQueue() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();

		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		boolean inserted = queue.offer(createDefaultElement());

		assertThat(inserted).isFalse();
		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		BlockingQueue<Element> anotherQueue = createDefaultProducerThrottlingBlockingQueue();
		queue.drainTo(anotherQueue);

		inserted = queue.offer(createDefaultElement());

		assertThat(inserted).isTrue();
		assertThat(queue).hasSize(1);
	}

	@Test
	public void shouldDrainQueue() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		BlockingQueue<Element> anotherQueue = createDefaultProducerThrottlingBlockingQueue();

		queue.drainTo(anotherQueue);

		assertThat(queue).isEmpty();
		assertThat(anotherQueue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test
	public void shouldDrainQueueButOnlyToMaxElements() {
		int maxElementsToDrain = calculateSizeWhenQueueHasMaxElementsFromAllProducers() / 2;
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		BlockingQueue<Element> anotherQueue = createDefaultProducerThrottlingBlockingQueue();

		queue.drainTo(anotherQueue, maxElementsToDrain);

		assertThat(queue).hasSize(maxElementsToDrain);
		assertThat(anotherQueue).hasSize(maxElementsToDrain);
	}

	@Test
	public void shouldNotDrainQueueWhenMaxElementsAreZero() {
		int maxElementsToDrain = 0;
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		BlockingQueue<Element> anotherQueue = createDefaultProducerThrottlingBlockingQueue();
		int sizeBeforeDrain = queue.size();

		queue.drainTo(anotherQueue, maxElementsToDrain);

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
		assertThat(queue).hasSize(sizeBeforeDrain);
	}

	@Test
	public void shouldCopeWithDrainingEmptyQueue() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		BlockingQueue<Element> anotherQueue = createDefaultProducerThrottlingBlockingQueue();

		queue.drainTo(anotherQueue);

		assertThat(queue).isEmpty();
		assertThat(anotherQueue).isEmpty();
	}

	@Test
	public void shouldCopeWithDrainingEmptyQueueUsingMaxElements() {
		int maxElementsToDrain = calculateSizeWhenQueueHasMaxElementsFromAllProducers() / 2;
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		BlockingQueue<Element> anotherQueue = createDefaultProducerThrottlingBlockingQueue();

		queue.drainTo(anotherQueue, maxElementsToDrain);

		assertThat(queue).isEmpty();
		assertThat(anotherQueue).isEmpty();
	}

	@Test
	public void shouldCopeWithDrainingQueueWhichSizeIsLessThanMaxElements() {
		int maxElementsToDrain = calculateSizeWhenQueueHasMaxElementsFromAllProducers() * 2;
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		BlockingQueue<Element> anotherQueue = createDefaultProducerThrottlingBlockingQueue();

		queue.drainTo(anotherQueue, maxElementsToDrain);

		assertThat(queue).isEmpty();
		assertThat(anotherQueue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void shouldThrowIllegalArgumentExceptionWhenBackingQueueAndDrainToQueueAreSame() {
		LinkedBlockingQueue<Element> backingQueue = new LinkedBlockingQueue<Element>();
		BlockingQueue<Element> queue = new ProducerThrottlingBlockingQueue<Element>(
				backingQueue);

		queue.drainTo(backingQueue);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void shouldThrowIllegalArgumentExceptionWhenBackingQueueAndDrainToQueueAreSameAndUsingMaxElements() {
		LinkedBlockingQueue<Element> backingQueue = new LinkedBlockingQueue<Element>();
		BlockingQueue<Element> queue = new ProducerThrottlingBlockingQueue<Element>(
				backingQueue);

		queue.drainTo(backingQueue,
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void shouldThrowIllegalArgumentExceptionWhenThisQueueAndDrainToQueueAreSame() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();

		queue.drainTo(queue);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void shouldThrowIllegalArgumentExceptionWhenThisQueueAndDrainToQueueAreSameAndUsingMaxElements() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();

		queue.drainTo(queue,
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test
	public void shouldThrowIllegalStateExceptionWhenAddingElementButViolatingCapacity() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		try {
			queue.add(createDefaultElement());
			fail("Should have thrown IllegalStateException");
		} catch (IllegalStateException e) {
			// Expected
		}

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void shouldThrowNullPointerExceptionWhenAddingNullValue() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();

		queue.add(null);
	}

	@Test
	public void shouldIterateOverQueueElements() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		Set<Element> identityBasedSet = Collections
				.newSetFromMap(new IdentityHashMap<Element, Boolean>());
		int numberOfIteratedElements = 0;

		for (Element element : queue) {
			identityBasedSet.add(element);
			numberOfIteratedElements++;

			if (!queue.contains(element)) {
				fail("Queue should have contained element");
			}
		}

		assertThat(identityBasedSet).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
		assertThat(numberOfIteratedElements).isEqualTo(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test(expectedExceptions = NoSuchElementException.class)
	public void shouldThrowNoSuchElementExceptionWhenAttemptingToInvokeNextOnIteratorFromAnEmptyQueue() {
		Iterator<Element> iterator = createDefaultProducerThrottlingBlockingQueue()
				.iterator();
		iterator.next();
	}

	@Test(expectedExceptions = IllegalStateException.class)
	public void shouldThrowIllegalStateExceptionWhenAttemptingToInvokeRemoveOnCompletelyNewIterator() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		Iterator<Element> iterator = queue.iterator();
		iterator.remove();
	}

	@Test
	public void shouldRemoveElementsFromQueueUsingIterator() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		Iterator<Element> iterator = queue.iterator();
		int numberOfIteratedElements = 0;

		while (iterator.hasNext()) {
			numberOfIteratedElements++;
			iterator.next();
			iterator.remove();
		}

		assertThat(numberOfIteratedElements).isEqualTo(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
		assertThat(queue).isEmpty();
	}

	@Test
	public void shouldPeekAtElement() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		List<Element> createdElements = verifyAndOfferAllElementsFromAllProducers(
				queue, PRODUCER_CAPACITY);

		Element element = queue.peek();

		assertThat(element).isSameAs(createdElements.get(0));
	}

	@Test
	public void shouldPeekAtNullWhenEmptyQueue() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();

		Element element = queue.peek();

		assertThat(element).isNull();
	}

	@Test
	public void shouldCalculateRemainingCapacity() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		assertThat(queue.remainingCapacity())
				.isEqualTo(
						Integer.MAX_VALUE
								- calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test
	public void shouldReturnCurrentSizeOfQueue() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY / 2);

		int numberOfInsertedElements = calculateSizeWhenQueueHasMaxElementsFromAllProducers() / 2;
		int expectedSize = calculateSizeWhenQueueHasMaxElementsFromAllProducers()
				- numberOfInsertedElements;

		assertThat(queue).hasSize(expectedSize);
	}

	@Test
	public void shouldReturnZeroWhenQueueIsEmpty() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();

		assertThat(queue).isEmpty();
	}

	@Test
	public void shouldRemoveElementsByEquality() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		Iterator<Element> createdElements = verifyAndOfferAllElementsFromAllProducers(
				queue, PRODUCER_CAPACITY).iterator();
		int numberOfRemovals = 0;

		while (!queue.isEmpty()) {
			boolean removed = queue.remove(createdElements.next());
			numberOfRemovals++;

			if (!removed) {
				fail("Should have removed the specified element");
			}
		}

		assertThat(queue).isEmpty();
		assertThat(numberOfRemovals).isEqualTo(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
		assertThat(createdElements.hasNext()).isFalse();
	}

	@Test
	public void shouldNotRemoveNonExistingElement() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		boolean removed = queue.remove(new Element("Dummy", PRODUCER_CAPACITY,
				ThrottlingPolicy.MUST_THROTTLE));

		assertThat(removed).isFalse();
		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
	}

	@Test
	public void shouldReturnArrayRepresentationOfQueue() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		List<Element> createdElements = verifyAndOfferAllElementsFromAllProducers(
				queue, PRODUCER_CAPACITY);

		Element[] destination = new Element[queue.size()];
		Element[] elements = queue.toArray(destination);

		assertThat(createdElements).hasSize(elements.length);
		assertThat(createdElements.containsAll(Arrays.asList(elements)))
				.isTrue();
		assertThat(destination).isSameAs(elements);
	}

	@Test
	public void shouldReturnArrayRepresentationOfQueueUsingNoArguments() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		List<Element> createdElements = verifyAndOfferAllElementsFromAllProducers(
				queue, PRODUCER_CAPACITY);

		Object[] elements = queue.toArray();

		assertThat(createdElements).hasSize(elements.length);
		assertThat(createdElements.containsAll(Arrays.asList(elements)))
				.isTrue();
	}

	@Test
	public void shouldClearElementsInQueue() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		assertThat(queue).isNotEmpty();

		queue.clear();

		assertThat(queue).isEmpty();
	}

	@Test
	public void shouldContainElement() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY / 2);
		Element element = new Element("Dummy", PRODUCER_CAPACITY,
				ThrottlingPolicy.MUST_THROTTLE);
		queue.add(element);

		boolean exists = queue.contains(element);

		assertThat(exists).isTrue();
	}

	@Test
	public void shouldNotContainNull() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		boolean exists = queue.contains(null);

		assertThat(exists).isFalse();
	}

	@Test
	public void shouldNotBeAbleToRemoveNull() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		boolean removed = queue.remove(null);

		assertThat(removed).isFalse();
	}

	@Test
	public void shouldReturnArrayRepresentationOfQueueWhenProvidingArrayWhichIsGreaterThanQueueSize() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		int newArraySize = queue.size() * 2;

		Element[] elements = queue.toArray(new Element[newArraySize]);

		assertThat(elements).hasSize(newArraySize);
		assertThat(elements[queue.size()]).isNull();
	}

	@Test
	public void shouldReturnArrayRepresentationOfQueueByProvidingSizeLessThanQueueSize() {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		List<Element> createdElements = verifyAndOfferAllElementsFromAllProducers(
				queue, PRODUCER_CAPACITY);

		Element[] destination = new Element[queue.size() / 2];
		Element[] elements = queue.toArray(destination);

		assertThat(createdElements).hasSize(elements.length);
		assertThat(createdElements.containsAll(Arrays.asList(elements)))
				.isTrue();
		assertThat(destination).isNotSameAs(elements);
	}

	@Test
	public void shouldTakeAllFromQueue() throws InterruptedException {
		BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);

		int numberOfPolledElements = 0;

		while (!queue.isEmpty()) {
			numberOfPolledElements++;
			Element element = queue.take();
			assertThat(element).isNotNull();
		}

		assertThat(numberOfPolledElements).isEqualTo(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());
		assertThat(queue).isEmpty();
	}

	@Test
	public void shouldCopeWithProducerCapacityGoesBeyondDefinedLimitDueToAttemptThrottle()
			throws Exception {
		int maxNumberOfAllowedElements = 1;
		final Thread testThread = Thread.currentThread();
		final CountDownLatch firstTwoTakenBarrier = new CountDownLatch(1);
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		Element attemptThrottleElement = new Element(
				DEFAULT_PRODUCER_IDENTIFIER, maxNumberOfAllowedElements,
				ThrottlingPolicy.ATTEMPT_THROTTLE);
		Element mustThrottleElement = new Element(DEFAULT_PRODUCER_IDENTIFIER,
				maxNumberOfAllowedElements, ThrottlingPolicy.MUST_THROTTLE);

		queue.offer(attemptThrottleElement);
		queue.offer(attemptThrottleElement);
		myExecutor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					long startTime = System.nanoTime();
					while (testThread.getState() != State.TIMED_WAITING
							&& (System.nanoTime() - startTime) < TimeUnit.NANOSECONDS
									.toSeconds(2)) {
						Thread.yield();
					}

					queue.take();
					queue.take();
					firstTwoTakenBarrier.countDown();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});

		queue.offer(mustThrottleElement, 10, TimeUnit.SECONDS);
		firstTwoTakenBarrier.await(10, TimeUnit.SECONDS);
		queue.poll();
		queue.add(mustThrottleElement);

		assertThat(queue).hasSize(1);
	}

	@Test
	public void shouldOfferElementToFullQueueUntilQueueIsCleared()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				queue.clear();
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurentPerformOfferElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldOfferElementToFullQueueUntilQueueIsDrained()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				queue.drainTo(new ArrayList<Element>());
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurentPerformOfferElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldOfferElementToFullQueueUntilQueueIsDrainedUsingMaxElements()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				int maxElements = queue.size() * 2;
				queue.drainTo(new ArrayList<Element>(), maxElements);
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurentPerformOfferElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldOfferElementToFullQueueUntilAnotherElementIsRemoved()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		final Element firstElement = verifyAndOfferAllElementsFromAllProducers(
				queue, PRODUCER_CAPACITY).iterator().next();
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				queue.remove(firstElement);
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurentPerformOfferElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldOfferElementToFullQueueUntilAnotherElementIsRemovedUsingIterator()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				Iterator<Element> iterator = queue.iterator();
				iterator.next();
				iterator.remove();
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurentPerformOfferElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldOfferElementToFullQueueUntilAnotherElementIsPolled()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				queue.poll();
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurentPerformOfferElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldOfferElementToFullQueueUntilAnotherElementIsPolledUsingSpecifiedTimeout()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				try {
					queue.poll(10, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					// Nothing we can do...
				}
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurentPerformOfferElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldOfferElementToFullQueueUntilAnotherElementIsTaken()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				try {
					queue.take();
				} catch (InterruptedException e) {
					// Nothing we can do...
				}
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurentPerformOfferElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldPutElementToFullQueueUntilQueueIsCleared()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				queue.clear();
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurrentPerformPutElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldPutElementToFullQueueUntilQueueIsDrained()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				queue.drainTo(new ArrayList<Element>());
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurrentPerformPutElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldPutElementToFullQueueUntilQueueIsDrainedUsingMaxElements()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				int maxElements = queue.size() * 2;
				queue.drainTo(new ArrayList<Element>(), maxElements);
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurrentPerformPutElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldPutElementToFullQueueUntilAnotherElementIsRemoved()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		final Element firstElement = verifyAndOfferAllElementsFromAllProducers(
				queue, PRODUCER_CAPACITY).iterator().next();
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				queue.remove(firstElement);
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurrentPerformPutElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldPutElementToFullQueueUntilAnotherElementIsRemovedUsingIterator()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				Iterator<Element> iterator = queue.iterator();
				iterator.next();
				iterator.remove();
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurrentPerformPutElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldPutElementToFullQueueUntilAnotherElementIsPolled()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				queue.poll();
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurrentPerformPutElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldPutElementToFullQueueUntilAnotherElementIsPolledUsingSpecifiedTimeout()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				try {
					queue.poll(10, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					// Nothing we can do...
				}
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurrentPerformPutElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldPutElementToFullQueueUntilAnotherElementIsTaken()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		verifyAndOfferAllElementsFromAllProducers(queue, PRODUCER_CAPACITY);
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				try {
					queue.take();
				} catch (InterruptedException e) {
					// Nothing we can do...
				}
			}
		};

		assertThat(queue).hasSize(
				calculateSizeWhenQueueHasMaxElementsFromAllProducers());

		concurrentPerformPutElementUntilSpaceIsAvailable(
				executionToEnableSpace, queue);
	}

	@Test
	public void shouldPollElementFromEmptyQueueUntilElementIsOffered()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		final Element elementToAdd = createDefaultElement();
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				queue.offer(elementToAdd);
			}
		};

		assertThat(queue).isEmpty();

		concurrentPerformPollUntilElementIsAvailable(executionToEnableSpace,
				queue, elementToAdd);
	}

	@Test
	public void shouldPollElementFromEmptyQueueUntilElementIsOfferedUsingSpecifiedTimeout()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		final Element elementToAdd = createDefaultElement();
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				try {
					queue.offer(elementToAdd, 10, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
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
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		final Element elementToAdd = createDefaultElement();
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				try {
					queue.put(elementToAdd);
				} catch (InterruptedException e) {
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
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		final Element elementToAdd = createDefaultElement();
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				queue.add(elementToAdd);
			}
		};

		assertThat(queue).isEmpty();

		concurrentPerformPollUntilElementIsAvailable(executionToEnableSpace,
				queue, elementToAdd);
	}

	@Test
	public void shouldTakeElementFromEmptyQueueUntilElementIsOffered()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		final Element elementToAdd = createDefaultElement();
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				queue.offer(elementToAdd);
			}
		};

		assertThat(queue).isEmpty();

		concurrentPerformTakeUntilElementIsAvailable(executionToEnableSpace,
				queue, elementToAdd);
	}

	@Test
	public void shouldTakeElementFromEmptyQueueUntilElementIsOfferedUsingSpecifiedTimeout()
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		final Element elementToAdd = createDefaultElement();
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				try {
					queue.offer(elementToAdd, 10, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
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
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		final Element elementToAdd = createDefaultElement();
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				try {
					queue.put(elementToAdd);
				} catch (InterruptedException e) {
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
			throws Exception {
		final BlockingQueue<Element> queue = createDefaultProducerThrottlingBlockingQueue();
		final Element elementToAdd = createDefaultElement();
		ExecutionInjection executionToEnableSpace = new ExecutionInjection() {
			@Override
			public void apply() {
				queue.add(elementToAdd);
			}
		};

		assertThat(queue).isEmpty();

		concurrentPerformTakeUntilElementIsAvailable(executionToEnableSpace,
				queue, elementToAdd);
	}

	private void concurrentPerformTakeUntilElementIsAvailable(
			ExecutionInjection executionToAddElementToQueue,
			final BlockingQueue<Element> queue, Element elementToBeAdded)
			throws Exception {
		final AtomicReference<Thread> concurrentThread = new AtomicReference<Thread>();
		final AtomicReference<Element> takenElement = new AtomicReference<Element>();
		final CountDownLatch reachedLockBarrier = new CountDownLatch(1);
		final CountDownLatch takenElementBarrier = new CountDownLatch(1);
		myExecutor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					concurrentThread.set(Thread.currentThread());
					reachedLockBarrier.countDown();
					Element element = queue.take();
					takenElement.set(element);
					takenElementBarrier.countDown();
				} catch (InterruptedException e) {
					// No point in doing anything...
				}
			}
		});

		reachedLockBarrier.await(10, TimeUnit.SECONDS);

		long startTime = System.nanoTime();
		while (concurrentThread.get().getState() != State.TIMED_WAITING
				&& (System.nanoTime() - startTime) < TimeUnit.NANOSECONDS
						.toSeconds(2)) {
			Thread.yield();
		}

		executionToAddElementToQueue.apply();

		takenElementBarrier.await(2, TimeUnit.SECONDS);

		assertThat(takenElement.get()).isSameAs(elementToBeAdded);
	}

	private void concurrentPerformPollUntilElementIsAvailable(
			ExecutionInjection executionToAddElementToQueue,
			final BlockingQueue<Element> queue, Element elementToBeAdded)
			throws Exception {
		final AtomicReference<Thread> concurrentThread = new AtomicReference<Thread>();
		final AtomicReference<Element> polledElement = new AtomicReference<Element>();
		final CountDownLatch reachedLockBarrier = new CountDownLatch(1);
		final CountDownLatch polledElementBarrier = new CountDownLatch(1);
		myExecutor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					concurrentThread.set(Thread.currentThread());
					reachedLockBarrier.countDown();
					Element element = queue.poll(30, TimeUnit.SECONDS);
					polledElement.set(element);
					polledElementBarrier.countDown();
				} catch (InterruptedException e) {
					// No point in doing anything...
				}
			}
		});

		reachedLockBarrier.await(10, TimeUnit.SECONDS);

		long startTime = System.nanoTime();
		while (concurrentThread.get().getState() != State.TIMED_WAITING
				&& (System.nanoTime() - startTime) < TimeUnit.NANOSECONDS
						.toSeconds(2)) {
			Thread.yield();
		}

		executionToAddElementToQueue.apply();

		polledElementBarrier.await(2, TimeUnit.SECONDS);

		assertThat(polledElement.get()).isSameAs(elementToBeAdded);
	}

	private void concurentPerformOfferElementUntilSpaceIsAvailable(
			ExecutionInjection executionToEnableSpace,
			final BlockingQueue<Element> queue) throws Exception {
		final AtomicReference<Thread> concurrentThread = new AtomicReference<Thread>();
		final AtomicBoolean offeredElement = new AtomicBoolean(false);
		final CountDownLatch reachedLockBarrier = new CountDownLatch(1);
		final CountDownLatch offeredElementBarrier = new CountDownLatch(1);
		myExecutor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					concurrentThread.set(Thread.currentThread());
					reachedLockBarrier.countDown();
					boolean isInserted = queue.offer(createDefaultElement(),
							30, TimeUnit.SECONDS);
					offeredElement.set(isInserted);
					offeredElementBarrier.countDown();
				} catch (InterruptedException e) {
					// No point in doing anything...
				}
			}
		});

		reachedLockBarrier.await(10, TimeUnit.SECONDS);

		long startTime = System.nanoTime();
		while (concurrentThread.get().getState() != State.TIMED_WAITING
				&& (System.nanoTime() - startTime) < TimeUnit.NANOSECONDS
						.toSeconds(2)) {
			Thread.yield();
		}

		executionToEnableSpace.apply();
		offeredElementBarrier.await(2, TimeUnit.SECONDS);

		assertThat(offeredElement.get()).isTrue();
	}

	private void concurrentPerformPutElementUntilSpaceIsAvailable(
			ExecutionInjection executionToEnableSpace,
			final BlockingQueue<Element> queue) throws Exception {
		final AtomicReference<Thread> concurrentThread = new AtomicReference<Thread>();
		final AtomicBoolean putElement = new AtomicBoolean(false);
		final CountDownLatch reachedLockBarrier = new CountDownLatch(1);
		final CountDownLatch putElementBarrier = new CountDownLatch(1);
		final Element elementToOffer = createDefaultElement();
		myExecutor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					concurrentThread.set(Thread.currentThread());
					reachedLockBarrier.countDown();
					queue.put(elementToOffer);
					putElement.set(true);
					putElementBarrier.countDown();
				} catch (InterruptedException e) {
					// No point in doing anything...
				}
			}
		});

		reachedLockBarrier.await(10, TimeUnit.SECONDS);

		long startTime = System.nanoTime();
		while (concurrentThread.get().getState() != State.TIMED_WAITING
				&& (System.nanoTime() - startTime) < TimeUnit.NANOSECONDS
						.toSeconds(2)) {
			Thread.yield();
		}

		executionToEnableSpace.apply();

		putElementBarrier.await(2, TimeUnit.SECONDS);

		assertThat(putElement.get()).isTrue();
	}

	private static int calculateSizeWhenQueueHasMaxElementsFromAllProducers() {
		int numberOfProducers = 3;
		return PRODUCER_CAPACITY * numberOfProducers;
	}

	private List<Element> verifyAndAddAllElementsFromAllProducers(
			BlockingQueue<Element> queue, int numberOfElements) {
		List<Element> createdElements = new ArrayList<Element>();
		createdElements.addAll(verifyAndAddAllElements(queue, numberOfElements,
				PRODUCER_IDENTIFIER_1));
		createdElements.addAll(verifyAndAddAllElements(queue, numberOfElements,
				PRODUCER_IDENTIFIER_2));
		createdElements.addAll(verifyAndAddAllElements(queue, numberOfElements,
				PRODUCER_IDENTIFIER_3));
		return createdElements;
	}

	private List<Element> verifyAndAddAllElements(BlockingQueue<Element> queue,
			int numberOfElements, Object producerIdentifier) {
		List<Element> createdElements = createElements(numberOfElements,
				producerIdentifier);

		for (Element e : createdElements) {
			boolean added = queue.add(e);

			if (!added) {
				fail("Should have inserted the specified element");
			}
		}

		return createdElements;
	}

	private List<Element> verifyAndOfferAllElementsFromAllProducers(
			BlockingQueue<Element> queue, int numberOfElements) {
		List<Element> createdElements = new ArrayList<Element>();
		createdElements.addAll(verifyAndOfferAllElements(queue,
				numberOfElements, PRODUCER_IDENTIFIER_1));
		createdElements.addAll(verifyAndOfferAllElements(queue,
				numberOfElements, PRODUCER_IDENTIFIER_2));
		createdElements.addAll(verifyAndOfferAllElements(queue,
				numberOfElements, PRODUCER_IDENTIFIER_3));
		return createdElements;
	}

	private List<Element> verifyAndOfferAllElements(
			BlockingQueue<Element> queue, int numberOfElements,
			Object producerIdentifier) {
		List<Element> createdElements = createElements(numberOfElements,
				producerIdentifier);

		for (Element e : createdElements) {
			boolean added = queue.offer(e);

			if (!added) {
				fail("Should have inserted the specified element");
			}
		}

		return createdElements;
	}

	private List<Element> verifyAndOfferAllElementsFromAllProducers(
			BlockingQueue<Element> queue, int numberOfElements, long timeout,
			TimeUnit unit) throws InterruptedException {
		List<Element> createdElements = new ArrayList<Element>();
		createdElements.addAll(verifyAndOfferAllElements(queue,
				numberOfElements, PRODUCER_IDENTIFIER_1, timeout, unit));
		createdElements.addAll(verifyAndOfferAllElements(queue,
				numberOfElements, PRODUCER_IDENTIFIER_2, timeout, unit));
		createdElements.addAll(verifyAndOfferAllElements(queue,
				numberOfElements, PRODUCER_IDENTIFIER_3, timeout, unit));
		return createdElements;
	}

	private List<Element> verifyAndOfferAllElements(
			BlockingQueue<Element> queue, int numberOfElements,
			Object producerIdentifier, long timeout, TimeUnit unit)
			throws InterruptedException {
		List<Element> createdElements = createElements(numberOfElements,
				producerIdentifier);

		for (Element e : createdElements) {
			boolean added = queue.offer(e, timeout, unit);

			if (!added) {
				fail("Should have inserted the specified element");
			}
		}

		return createdElements;
	}

	private List<Element> putAllElementsFromAllProducers(
			BlockingQueue<Element> queue, int numberOfElements)
			throws InterruptedException {
		List<Element> createdElements = new ArrayList<Element>();
		createdElements.addAll(putAllElements(queue, numberOfElements,
				PRODUCER_IDENTIFIER_1));
		createdElements.addAll(putAllElements(queue, numberOfElements,
				PRODUCER_IDENTIFIER_2));
		createdElements.addAll(putAllElements(queue, numberOfElements,
				PRODUCER_IDENTIFIER_3));
		return createdElements;
	}

	private List<Element> putAllElements(BlockingQueue<Element> queue,
			int numberOfElements, Object producerIdentifier)
			throws InterruptedException {
		List<Element> createdElements = createElements(numberOfElements,
				producerIdentifier);

		for (Element e : createdElements) {
			queue.put(e);
		}

		return createdElements;
	}

	private List<Element> createElements(int numberOfElements,
			Object producerIdentifier) {
		List<Element> createdElements = new ArrayList<Element>(numberOfElements);

		for (int i = 0; i < numberOfElements; ++i) {
			createdElements.add(new Element(producerIdentifier,
					PRODUCER_CAPACITY, ThrottlingPolicy.MUST_THROTTLE));
		}

		return createdElements;
	}

	private Element createDefaultElement() {
		return new Element(DEFAULT_PRODUCER_IDENTIFIER, PRODUCER_CAPACITY,
				ThrottlingPolicy.MUST_THROTTLE);
	}

	private BlockingQueue<Element> createDefaultProducerThrottlingBlockingQueue() {
		return new ProducerThrottlingBlockingQueue<Element>(
				new LinkedBlockingQueue<Element>());
	}

	private static class Element implements ProducerThrottlingQueueElement {
		private final Object myProducerIdentifier;
		private final int myProducerCapacity;
		private final ThrottlingPolicy myThrottlingPolicy;

		Element(Object producerIdentifier, int producerCapacity,
				ThrottlingPolicy throttlingPolicy) {
			myProducerIdentifier = producerIdentifier;
			myProducerCapacity = producerCapacity;
			myThrottlingPolicy = throttlingPolicy;
		}

		@Override
		public String toString() {
			return "" + myProducerIdentifier;
		}

		@Override
		public Object getProducerIdentifier() {
			return myProducerIdentifier;
		}

		@Override
		public int getAllowedProducerCapacity() {
			return myProducerCapacity;
		}

		@Override
		public ThrottlingPolicy getThrottlingPolicy() {
			return myThrottlingPolicy;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + myProducerCapacity;
			result = prime
					* result
					+ ((myProducerIdentifier == null) ? 0
							: myProducerIdentifier.hashCode());
			result = prime
					* result
					+ ((myThrottlingPolicy == null) ? 0 : myThrottlingPolicy
							.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Element other = (Element) obj;
			if (myProducerCapacity != other.myProducerCapacity)
				return false;
			if (myProducerIdentifier == null) {
				if (other.myProducerIdentifier != null)
					return false;
			} else if (!myProducerIdentifier.equals(other.myProducerIdentifier))
				return false;
			if (myThrottlingPolicy != other.myThrottlingPolicy)
				return false;
			return true;
		}
	}

	/**
	 * A functional interface for injecting execution.
	 * 
	 */
	private static interface ExecutionInjection {
		/**
		 * Applies the execution
		 * 
		 */
		public void apply();
	}
}
