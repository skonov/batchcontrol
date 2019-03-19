package batchcontrol.services.iterators;

import java.util.Date;

import batchcontrol.services.SchedulerTask;

/**
 * Implementations of <code>ScheduleIterator</code> specify a schedule as a series of 
 * <code>java.util.Date</code> objects.
 */
public interface SchedulerIterator {
	
	public static final String NEW     = "new";
	public static final String ACTIVE  = "active";
	public static final String UPDATED = "updated";
	public static final String DELETED = "deleted";
	
	/**
	 * Returns the next time that the related {@link SchedulerTask} should be run.
	 * @return the next time of execution
	 */
	public Date next();
	
	public int getHour();
	public int getMinute();
	public int getSecond();
	public String getDays();
	public String getType();
	public Object clone() throws CloneNotSupportedException;
	public String getState();
	public void setState(String state);
	public int getId();
	
}

