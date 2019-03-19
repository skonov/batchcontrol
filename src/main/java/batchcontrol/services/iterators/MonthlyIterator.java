package batchcontrol.services.iterators;

import java.util.Calendar;
import java.util.Date;

import batchcontrol.services.BatchControlServiceImpl;

public class MonthlyIterator implements SchedulerIterator {
	private final int dayOfMonth, hourOfDay, minute, second;
	private final Calendar calendar = Calendar.getInstance();
	private final int id;
	private String state = SchedulerIterator.ACTIVE;
	
	public MonthlyIterator(int dayOfMonth, int hourOfDay, int minute, int second, int id) {
		this(dayOfMonth, hourOfDay, minute, second, new Date(), id);
	}

	public MonthlyIterator(int dayOfMonth, int hourOfDay, int minute, int second, Date date, int id) {
		this.id = id;
		this.hourOfDay = hourOfDay;
		this.minute = minute;
		this.second = second;
		this.dayOfMonth = dayOfMonth;
		calendar.setTime(date);
		calendar.set(Calendar.DAY_OF_MONTH, dayOfMonth);
		calendar.set(Calendar.HOUR_OF_DAY, hourOfDay);
		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.SECOND, second);
		calendar.set(Calendar.MILLISECOND, 0);
		
		//go one month back to schedule for immediate execution
		if (calendar.getTime().after(date)) {
			calendar.add(Calendar.MONTH, -1);
		}
	}

	public Date next() {
		calendar.add(Calendar.MONTH, 1);
		return calendar.getTime();
	}
	
	public String toString() {
		return "[Monthly: day="+dayOfMonth+"h="+hourOfDay+", m="+minute+", s="+second+"]";
	}

	public String getDays() {
		return String.valueOf(dayOfMonth);
	}

	public int getHour() {
		return hourOfDay;
	}

	public int getMinute() {
		return minute;
	}

	public int getSecond() {
		return second;
	}

	public String getType() {
		return BatchControlServiceImpl.MONTHLY_TYPE;
	}

	public int getId() {
		return id;
	}

	public Object clone() {
		return new MonthlyIterator(dayOfMonth, hourOfDay, minute, second, id);
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

}
