package batchcontrol.services.iterators;

import java.util.Calendar;
import java.util.Date;

import batchcontrol.services.BatchControlImpl;

public class FirstBusinessDayOfMonthIterator implements SchedulerIterator {
	private final int hourOfDay, minute, second;
	private final Calendar calendar = Calendar.getInstance();
	private final int id;
	private String state = SchedulerIterator.ACTIVE;
	
	public FirstBusinessDayOfMonthIterator(int hourOfDay, int minute, int second, int id) {
		this(hourOfDay, minute, second, new Date(), id);
	}
	
	public FirstBusinessDayOfMonthIterator(int hourOfDay, int minute, int second, Date date, int id) {
		this.id = id;
		this.hourOfDay = hourOfDay;
		this.minute = minute;
		this.second = second;
		calendar.setTime(date);
		calendar.set(Calendar.HOUR_OF_DAY, hourOfDay);
		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.SECOND, second);
		calendar.set(Calendar.MILLISECOND, 0);
		
		rollToFirstBusinessDay(calendar);
		if(calendar.getTime().before(date)) {
			calendar.add(Calendar.MONTH, 1);
			rollToFirstBusinessDay(calendar);
		}
	}

	public Date next() {
		calendar.add(Calendar.MONTH, 1);
		rollToFirstBusinessDay(calendar);
		return calendar.getTime();
	}
	
	public String toString() {
		return "[FirstBusinessDayOfMonthIterator: h="+hourOfDay+", m="+minute+", s="+second+"]";
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
		return BatchControlImpl.FIRST_BUSINESS_DAY_OF_MONTH_TYPE;
	}

	public int getId() {
		return id;
	}

	public Object clone() {
		return new FirstBusinessDayOfMonthIterator(this.hourOfDay, this.minute, this.second, this.id);
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	private boolean isWeekend(Calendar calendar) {
		if(calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY)	{
			return true;
		}
		if(calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
			return true;
		}
		return false;
	}
	
	private void rollToFirstBusinessDay(Calendar calendar) {
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		while(isWeekend(calendar)) {
			calendar.add(Calendar.DATE, 1);
		}
	}

}

