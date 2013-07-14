package gakesson.util.concurrent;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A thread-safe, non-blocking and lock-free implementation of a stack. The
 * implementation is linked-based and has no capacity-restriction. Note that
 * certain operations are not constant in time.
 * 
 * @author Gustav Akesson - gustav.r.akesson@gmail.com
 * 
 * @param <E>
 *            The type of elements held in this collection.
 */
public class NonBlockingStack<E>
{
    private final AtomicReference<Node> myTop = new AtomicReference<Node>();

    /**
     * Pushes the provided element on top of this stack.
     * 
     * @param e
     *            The element to push.
     */
    public void push(E e)
    {
        boolean continuePush = true;

        while (continuePush)
        {
            Node top = myTop.get();
            Node newTop = new Node(e, top);
            continuePush = !myTop.compareAndSet(top, newTop);
        }
    }

    /**
     * Returns and removes the top of this stack, or {@code null} if the stack
     * is empty.
     * 
     * @return The element at the top of this stack, or {@code null} if the
     *         stack is empty.
     */
    public E pop()
    {
        boolean continuePop = true;
        E topElement = null;

        while (continuePop)
        {
            Node topNode = myTop.get();

            if (topNode == null)
            {
                break;
            }

            topElement = topNode.getElement();
            Node newTopNode = topNode.getNext();
            continuePop = !myTop.compareAndSet(topNode, newTopNode);
        }

        return topElement;
    }

    /**
     * Returns, but does not remove, the top of this stack, or {@code null} if
     * the stack is empty.
     * 
     * @return The element at the top of this stack, or {@code null} if the
     *         stack is empty.
     */
    public E peek()
    {
        Node top = myTop.get();
        return top == null ? null : top.getElement();
    }

    /**
     * Verifies whether or not this stack is empty. This method is constant in
     * time.
     * 
     * @return
     */
    public boolean isEmpty()
    {
        return myTop.get() == null;
    }

    /**
     * Returns the size of this stack. Note that this method is NOT constant in
     * time since the stack has to be traversed in order to calculate the stack
     * size.
     * 
     * @return The size of this stack.
     */
    public int size()
    {
        Node currentNode = myTop.get();
        int size = 0;

        while (currentNode != null)
        {
            ++size;
            currentNode = currentNode.getNext();
        }

        return size;
    }

    /**
     * An internal class representing a linked node in the stack.
     * 
     */
    private class Node
    {
        private final E myElement;
        private final Node myNext;

        /**
         * Creates a new {@link Node} instance using the provided input.
         * 
         * @param element
         * @param next
         */
        Node(E element, Node next)
        {
            myElement = element;
            myNext = next;
        }

        /**
         * Returns the element held by this specific node.
         * 
         * @return
         */
        E getElement()
        {
            return myElement;
        }

        /**
         * Returns the reference to the next node in the stack.
         * 
         * @return
         */
        Node getNext()
        {
            return myNext;
        }
    }
}
