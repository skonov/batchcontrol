package batchcontrol.service.iterators;

import java.util.Calendar;
import java.util.Date;

import batchcontrol.service.BatchControlImpl;

/**
 * A <code>DailyIterator</code> returns a sequence of dates on subsequent days
 * representing the same time each day.
 */
public class DailyIterator implements SchedulerIterator {
	private final int hourOfDay, minute, second;
	private final Calendar calendar = Calendar.getInstance();
	private final int id;
	private String state = SchedulerIterator.ACTIVE;
	

	public DailyIterator(int hourOfDay, int minute, int second, int id) {
		this(hourOfDay, minute, second, new Date(), id);
	}

	public DailyIterator(int hourOfDay, int minute, int second, Date date, int id) {
		this.id = id;
		this.hourOfDay = hourOfDay;
		this.minute = minute;
		this.second = second;
		calendar.setTime(date);
		calendar.set(Calendar.HOUR_OF_DAY, hourOfDay);
		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.SECOND, second);
		calendar.set(Calendar.MILLISECOND, 0);
		if (!calendar.getTime().before(date)) {
			calendar.add(Calendar.DATE, -1);
		}
	}

	public Date next() {
		calendar.add(Calendar.DATE, 1);
		return calendar.getTime();
	}
	
	public String toString() {
		return "[Daily: h="+hourOfDay+", m="+minute+", s="+second+"]";
	}

	public String getDays() {
		return null;
	}

	public int getHour() {
		return this.hourOfDay;
	}

	public int getMinute() {
		return this.minute;
	}

	public int getSecond() {
		return this.second;
	}

	public String getType() {
		return BatchControlImpl.DAILY_TYPE;
	}

	public int getId() {
		return id;
	}

	public Object clone() {
		return new DailyIterator(this.hourOfDay, this.minute, this.second, this.id);
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

}
