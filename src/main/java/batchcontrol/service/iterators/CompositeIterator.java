package batchcontrol.service.iterators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Date;



/**
 * A <code>CompositeIterator</code> combines a number of {@link SchedulerIterator}s
 * into a single {@link SchedulerIterator}. Duplicate dates are removed.
 */
public class CompositeIterator implements SchedulerIterator {

	private List<Date> orderedTimes = new ArrayList<Date>();
	private List<SchedulerIterator> orderedIterators = new ArrayList<SchedulerIterator>();

	public CompositeIterator(SchedulerIterator[] scheduleIterators) {
		for (int i = 0; i < scheduleIterators.length; i++) {
			insert(scheduleIterators[i]);
		}
	}

	private void insert(SchedulerIterator scheduleIterator) {
		Date time = scheduleIterator.next();
		if (time == null) {
			return;
		}
		int index = Collections.binarySearch(orderedTimes, time);
		if (index < 0) { // time was not found
			index = -index - 1;
		}
		orderedTimes.add(index, time);
		orderedIterators.add(index, scheduleIterator);
	}

	public synchronized Date next() {
		Date next = null;
		while ( !orderedTimes.isEmpty() && (next == null || next.equals((Date) orderedTimes.get(0))) ) {
			next = (Date) orderedTimes.remove(0);
			insert((SchedulerIterator) orderedIterators.remove(0));
		}
		return next;
	}

	public String getDays() {
		throw new UnsupportedOperationException();
	}

	public int getHour() {
		throw new UnsupportedOperationException();
	}

	public int getMinute() {
		throw new UnsupportedOperationException();
	}

	public int getSecond() {
		throw new UnsupportedOperationException();
	}

	public String getType() {
		throw new UnsupportedOperationException();
	}
	
	public Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException();
	}

	public String getState() throws UnsupportedOperationException {
		return null;
	}

	public void setState(String state) throws UnsupportedOperationException {
	}

	public int getId() throws UnsupportedOperationException {
		return 0;
	}

}
