package gakesson.util.concurrent;

import java.util.concurrent.Semaphore;

/**
 * A {@link Semaphore} extension in order to be able to reduce the permits below
 * zero.
 * 
 */
public class AdaptiveSemaphore extends Semaphore {
	private static final long serialVersionUID = 8938254683748540380L;

	/**
	 * Creates a new {@link AdaptiveSemaphore} using the provided input and
	 * default access policy
	 * 
	 * @permits The initial number of permits available. This value may be
	 *          negative, in which case releases must occur before any acquires
	 *          will be granted.
	 */
	public AdaptiveSemaphore(int permits) {
		this(permits, false);
	}

	/**
	 * Creates a new {@link AdaptiveSemaphore} using the provided input.
	 * 
	 * @permits The initial number of permits available. This value may be
	 *          negative, in which case releases must occur before any acquires
	 *          will be granted.
	 * @param fair {@code true} if this semaphore will guarantee first-in
	 *            first-out granting of permits under contention, else
	 *            {@code false}.
	 */
	public AdaptiveSemaphore(int permits, boolean fair) {
		super(permits, fair);
	}

	/**
	 * Decrements the permits by one(1). This might have the affect that the
	 * current {@link Semaphore} permits go below zero.
	 * 
	 */
	public void decrementPermit() {
		reducePermits(1);
	}
}