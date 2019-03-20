package batchcontrol.services.iterators;

import java.util.Calendar;
import java.util.Date;

import batchcontrol.services.BatchControlImpl;


public class HourlyIterator implements SchedulerIterator {
	private final int minute, second;
	private final Calendar calendar = Calendar.getInstance();
	private final int id;
	private String state = SchedulerIterator.ACTIVE;

	public HourlyIterator(int minute, int second, int id) {
		this(minute, second, new Date(), id);
	}

	public HourlyIterator(int minute, int second, Date date, int id) {
		this.id = id;
		this.minute = minute;
		this.second = second;
		calendar.setTime(date);
		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.SECOND, second);
		calendar.set(Calendar.MILLISECOND, 0);
		if (calendar.getTime().after(date)) {
			calendar.add(Calendar.HOUR, -1);
		}
	}
	
	public Date next() {
		calendar.add(Calendar.HOUR, 1);
		return calendar.getTime();
	}
	
	public String toString() {
		return "[Hourly: m="+minute+", s="+second+"]";
	}

	public String getDays() {
		return null;
	}

	public int getHour() {
		return 0;
	}

	public int getMinute() {
		return this.minute;
	}

	public int getSecond() {
		return this.second;
	}

	public String getType() {
		return BatchControlImpl.HOURLY_TYPE;
	}

	public int getId() {
		return id;
	}

	public Object clone() {
		return new HourlyIterator(this.minute, this.second, this.id);
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

}
