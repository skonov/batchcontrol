package batchcontrol.services;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import batchcontrol.services.iterators.SchedulerIterator;


/**
 * A facility for threads to schedule recurring tasks for future execution in 
 * a background thread.
 * <p>
 * This class is thread-safe: multiple threads can share a single <code>Scheduler</code> object 
 * without the need for external synchronization.
 * <p>
 * Implementation note: internally <code>Scheduler</code> uses a <code>java.util.Timer</code> 
 * to schedule tasks.
 */
public class Scheduler {
	private static final Logger log = Logger.getLogger(Scheduler.class);

	class SchedulerTimerTask extends TimerTask {
		private SchedulerTask schedulerTask;
		private SchedulerIterator iterator;

		public SchedulerTimerTask(SchedulerTask schedulerTask, SchedulerIterator iterator) {
			this.schedulerTask = schedulerTask;
			this.iterator = iterator;
		}

		public void run() {
			schedulerTask.run();
			reschedule(schedulerTask, iterator);
		}
	}

	private final Timer timer = new Timer();
	protected boolean stopTask;

	public Scheduler() {
	}

	/**
	 * Terminates this <code>Scheduler</code>, discarding any currently scheduled tasks. 
	 * Does not interfere with a currently executing task (if it exists). Once a scheduler 
	 * has been terminated, its execution thread terminates gracefully, and no more tasks 
	 * may be scheduled on it.
	 * <p>
	 * Note that calling this method from within the run method of a scheduler task that 
	 * was invoked by this scheduler absolutely guarantees that the ongoing task execution 
	 * is the last task execution that will ever be performed by this scheduler.
	 * <p>
	 * This method may be called repeatedly; the second and subsequent calls have no effect.
	 */
	public void cancel() {
		timer.cancel();
	}
	
	protected void stopTask() {
		stopTask = true;
	}

	/**
	 * Schedules the specified task for execution according to the specified schedule. 
	 * If times specified by the <code>ScheduleIterator</code> are in the past they are 
	 * scheduled for immediate execution.
	 * <p>
	 * @param schedulerTask task to be scheduled
	 * @param iterator iterator that describes the schedule
	 * @throws IllegalStateException if task was already scheduled or cancelled, 
	 *         scheduler was cancelled, or scheduler thread terminated.
	 */
	public boolean schedule(SchedulerTask schedulerTask, SchedulerIterator iterator) {
		Date time = iterator.next();
		boolean result = true;
		if (time == null) {
			schedulerTask.cancel();
			result = false;
			log.debug("SCHEDULE: cancelled " + schedulerTask + " - scheduler returned no time.");
		} else {
			synchronized(schedulerTask.lock) {
				if (schedulerTask.state != SchedulerTask.VIRGIN) {
					throw new IllegalStateException("Task already scheduled or cancelled");
				}
				schedulerTask.state = SchedulerTask.SCHEDULED;
				schedulerTask.timerTask = new SchedulerTimerTask(schedulerTask, iterator);
				timer.schedule(schedulerTask.timerTask, time);
				log.debug("SCHEDULE: scheduled " + schedulerTask);
			}
		}
		return result;
	}
	
	public void scheduleOnce(final SchedulerTask schedulerTask) {
		new Timer().schedule(new TimerTask() {
			public void run() {
				schedulerTask.run();
			}
		}, new Date());
	}

	private void reschedule(SchedulerTask schedulerTask, SchedulerIterator iterator) {
		Date time = iterator.next();
		if (time == null) {
			schedulerTask.cancel();
		} else {
			synchronized(schedulerTask.lock) {
				if (schedulerTask.state != SchedulerTask.CANCELLED) {
					schedulerTask.timerTask = new SchedulerTimerTask(schedulerTask, iterator);
					timer.schedule(schedulerTask.timerTask, time);
					log.debug("Rescheduled " + schedulerTask);
				}
			}
		}
	}

}

