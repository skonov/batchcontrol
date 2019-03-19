package batchcontrol.services.iterators;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import batchcontrol.services.BatchControlServiceImpl;


/**
 * A <code>RestrictedDailyIterator</code> returns a sequence of dates on
 * subsequent days (restricted to a set of days, e.g. weekdays only)
 * representing the same time each day.
 */
public class RestrictedDailyIterator implements SchedulerIterator {
	private final int[] days;
	private final int hourOfDay, minute, second;
	private final Calendar calendar = Calendar.getInstance();
	private final int id;
	private String state = SchedulerIterator.ACTIVE;

	public RestrictedDailyIterator(int hourOfDay, int minute, int second, int[] days, int id) {
		this(hourOfDay, minute, second, days, new Date(), id);
	}
	
	public RestrictedDailyIterator(int hourOfDay, int minute, int second, int[] days, Date date, int id) {
		if(days.length==0) throw new IllegalArgumentException("days could not be empty for restricted daily iterator.");
		
		this.id = id;
		this.hourOfDay = hourOfDay;
		this.minute = minute;
		this.second = second;
		this.days = (int[]) days.clone();
		Arrays.sort(this.days);
		
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
		do {
			calendar.add(Calendar.DATE, 1);
		} while (Arrays.binarySearch(days, calendar.get(Calendar.DAY_OF_WEEK)) < 0);
		return calendar.getTime();
	}

	public String toString() {
		return "[RestrictedDaily: h="+getHour()+", m="+getMinute()+", s="+getSecond()+", days="+getDays()+"]";
	}

	public String getDays() {
		String days = "";
		for(int i=0; i<this.days.length; i++) {
			days += this.days[i];
		}
		return days;
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
		return BatchControlServiceImpl.RESTRICTED_DAILY_TYPE;
	}
	
	public int getId() {
		return id;
	}
	
	public Object clone() {
		return new RestrictedDailyIterator(this.hourOfDay, this.minute, this.second, this.days, this.id);
	}
	
	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}
	
}

