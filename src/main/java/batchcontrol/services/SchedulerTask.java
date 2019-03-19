package batchcontrol.services;

import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;



/**
 * A task that can be scheduled for recurring execution by a {@link Scheduler}.
 */
public abstract class SchedulerTask implements Runnable {
	private static final Logger log = Logger.getLogger(SchedulerTask.class);
	final Object lock = new Object();

	int state = VIRGIN;
	public static final int VIRGIN = 0;
	public static final int SCHEDULED = 1;
	public static final int CANCELLED = 2;
	public static final int IDLE = 3;
	public static final int BUSY = 4;
	
	public final static String LOGGER_KEY = "ApplicationName";

	/** if true run() method will exit */
	private boolean stop;

	protected BatchControlServiceImpl service;

	public void setService(BatchControlServiceImpl service) {
		this.service = service;
	}

	TimerTask timerTask;

	/**
	 * Creates a new scheduler task.
	 */
	protected SchedulerTask() {
	}

	public void run() {
		// active flag can be changed directly in database after the batch has started
		try {
			String className = this.getClass().getName();
			if(service.getActive(className)==0) {
				log.info("Batch "+className+" is not active: stopping...");
				service.stopBatch(className);
			}
		} catch (Exception e) {
			log.error(e.toString(), e);
		}
		
		if(stop) {
			log.info("Running task "+getName()+": task stopped.");
			return;
		}
		// inform the service that process has started
		service.processStarted();
		MDC.put(LOGGER_KEY, "BATCH_STATUS");
		log.info("Running task " + getName() + ": started.");
		MDC.remove(LOGGER_KEY);
		try {
			MDC.put(LOGGER_KEY, getName());
			state = BUSY;
			process();
		} catch (Throwable e) {
			log.error("Running task "+getName()+": "+e, e);
		} finally {
			state = IDLE;
			service.processStopped();
			MDC.remove(LOGGER_KEY);
		}
		MDC.put(LOGGER_KEY, "BATCH_STATUS");
		log.info("Running task " + getName() + ": finished.");
		MDC.remove(LOGGER_KEY);
	}
	
	protected abstract String getName();

	/**
	 * The action to be performed by this scheduler task.
	 */
	protected abstract void process();

	public void setStop(boolean stop) {
		this.stop = stop;
	}

	/**
	 * Cancels this scheduler task.
	 * <p>
	 * This method may be called repeatedly; the second and subsequent calls have no effect.
	 * 
	 * @return true if this task was already scheduled to run
	 */
	public boolean cancel() {
		synchronized(lock) {
			if (timerTask != null) {
				timerTask.cancel();
			}
			boolean result = (state == SCHEDULED);
			state = CANCELLED;
			log.debug("Task "+getName()+" was cancelled: result="+result);
			return result;
		}
	}

	/**
	 * Returns the <i>scheduled</i> execution time of the most recent actual execution of 
	 * this task. (If this method is invoked while task execution is in progress, the 
	 * return value is the scheduled execution time of the ongoing task execution.)
	 * 
	 * @return the time at which the most recent execution of this task was scheduled to 
	 * occur, in the format returned by <code>Date.getTime()</code>. The return value is 
	 * undefined if the task has yet to commence its first execution.
	 */
	public long scheduledExecutionTime() {
		synchronized(lock) {
			return timerTask == null ? 0 : timerTask.scheduledExecutionTime();
		}
	}
	
	public int getState() {
		return state;
	}

}
