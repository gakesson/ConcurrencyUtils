package gakesson.util.concurrent;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class NonBlockingStackTest
{
    @BeforeMethod
    public void setupTestCase()
    {
        Element.ourInstanceCounter.set(0);
    }

    @Test
    public void shouldPushElement()
    {
        NonBlockingStack<Element> stack = new NonBlockingStack<Element>();

        Element element = new Element();
        stack.push(element);

        assertThat(stack.peek()).isSameAs(element);
    }

    @Test
    public void shouldPushAndPopElementsInLIFO()
    {
        int numberOfElements = 100;
        NonBlockingStack<Element> stack = new NonBlockingStack<Element>();
        List<Element> elements = createAndPushElements(numberOfElements, stack);
        int reverseIndex = elements.size() - 1;

        for (int i = reverseIndex; i >= 0; --i)
        {
            Element element = stack.pop();
            assertThat(element).isSameAs(elements.get(i));
        }
    }

    @Test
    public void shouldReturnCorrectSizeOfStack()
    {
        int numberOfElements = 100;
        NonBlockingStack<Element> stack = new NonBlockingStack<Element>();
        createAndPushElements(numberOfElements, stack);

        assertThat(stack.size()).isEqualTo(numberOfElements);
    }

    @Test
    public void shouldReturnZeroSizeWhenStackIsEmpty()
    {
        NonBlockingStack<Element> stack = new NonBlockingStack<Element>();

        assertThat(stack.size()).isZero();
    }

    @Test
    public void shouldReturnIsEmptyWhenStackHasNoElements()
    {
        NonBlockingStack<Element> stack = new NonBlockingStack<Element>();

        assertThat(stack.isEmpty()).isTrue();
    }

    @Test
    public void shouldNotReturnIsEmptyWhenStackHasElements()
    {
        int numberOfElements = 100;
        NonBlockingStack<Element> stack = new NonBlockingStack<Element>();
        createAndPushElements(numberOfElements, stack);

        assertThat(stack.isEmpty()).isFalse();
    }

    @Test
    public void concurrentShouldPushAndPopElements() throws Exception
    {
        final int numberOfElements = 10000;
        final NonBlockingStack<Element> stack = new NonBlockingStack<Element>();
        ExecutorService executor = Executors.newCachedThreadPool();
        int parallelism = 20;
        final CountDownLatch resultBarrier = new CountDownLatch(parallelism);
        final AtomicBoolean wasOK = new AtomicBoolean(true);

        for (int i = 0; i < parallelism; ++i)
        {
            executor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        for (int x = 0; x < numberOfElements; ++x)
                        {
                            stack.push(new Element());
                        }

                        for (int x = 0; x < numberOfElements; ++x)
                        {
                            Element element = stack.pop();

                            assertThat(element).isNotNull();
                        }
                    } 
                    catch (Throwable t)
                    {
                        wasOK.set(false);
                        throw new RuntimeException(t);
                    } 
                    finally
                    {
                        resultBarrier.countDown();
                    }
                }
            });
        }

        resultBarrier.await(5, TimeUnit.SECONDS);

        assertThat(stack.isEmpty()).isTrue();
        assertThat(wasOK.get()).isTrue();

        executor.shutdownNow();
    }

    private List<Element> createAndPushElements(int numberOfElements, NonBlockingStack<Element> stack)
    {
        List<Element> createdElements = new ArrayList<Element>(numberOfElements);

        for (int i = 0; i < numberOfElements; ++i)
        {
            Element element = new Element();
            createdElements.add(element);
            stack.push(element);
        }

        return createdElements;
    }

    /**
     * A class type to be held by the {@link NonBlockingStack}.
     *
     */
    private static class Element
    {
        private static final AtomicInteger ourInstanceCounter = new AtomicInteger();

        private final int myInstanceId;

        Element()
        {
            myInstanceId = ourInstanceCounter.incrementAndGet();
        }

        @Override
        public String toString()
        {
            return "Element with ID=" + myInstanceId;
        }
    }
}
