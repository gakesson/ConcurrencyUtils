package gakesson.util.concurrent;

import static org.fest.assertions.Assertions.assertThat;

import org.testng.annotations.Test;

public class AdaptiveSemaphoreTest {
	@Test
	public void shouldBeAbleToDecrementPermitsAndThenRelease() {
		int numberOfPermits = 100;
		AdaptiveSemaphore semaphore = new AdaptiveSemaphore(numberOfPermits);

		for (int i = 0; i < numberOfPermits * 2; ++i) {
			boolean acquired = semaphore.tryAcquire();

			if (!acquired) {
				semaphore.decrementPermit();
			}
		}

		assertThat(semaphore.tryAcquire()).isFalse();

		for (int i = 0; i < numberOfPermits; ++i) {
			semaphore.release();
		}

		assertThat(semaphore.availablePermits()).isZero();
		assertThat(semaphore.tryAcquire()).isFalse();

		for (int i = 0; i < numberOfPermits; ++i) {
			semaphore.release();
		}

		assertThat(numberOfPermits).isEqualTo(semaphore.availablePermits());
		assertThat(semaphore.tryAcquire()).isTrue();
	}
}
