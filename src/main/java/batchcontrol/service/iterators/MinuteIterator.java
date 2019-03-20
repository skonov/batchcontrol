package batchcontrol.service.iterators;

import java.util.Calendar;
import java.util.Date;

import batchcontrol.service.BatchControlImpl;


public class MinuteIterator implements SchedulerIterator {
	private final int second;
	private final Calendar calendar = Calendar.getInstance();
	private final int id;
	private String state = SchedulerIterator.ACTIVE;

	public MinuteIterator(int second, int id) {
		this(second, new Date(), id);
	}

	public MinuteIterator(int second, Date date, int id) {
		this.id = id;
		this.second = second;
		calendar.setTime(date);
		calendar.set(Calendar.SECOND, second);
		calendar.set(Calendar.MILLISECOND, 0);
		if (calendar.getTime().after(date)) {
			calendar.add(Calendar.MINUTE, -1);
		}
	}
	
	public Date next() {
		calendar.add(Calendar.MINUTE, 1);
		return calendar.getTime();
	}
	
	public String toString() {
		return "[Minute: s="+second+"]";
	}

	public String getDays() {
		return null;
	}

	public int getHour() {
		return 0;
	}

	public int getMinute() {
		return 0;
	}

	public int getSecond() {
		return this.second;
	}

	public String getType() {
		return BatchControlImpl.MINUTE_TYPE;
	}

	public int getId() {
		return id;
	}
	
	public Object clone() {
		return new MinuteIterator(this.second, this.id);
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}
	
	
}
