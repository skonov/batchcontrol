package batchcontrol.service;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import batchcontrol.service.iterators.CompositeIterator;
import batchcontrol.service.iterators.SchedulerIterator;

public class DefaultBatch {
	
	private static final Logger log = Logger.getLogger(DefaultBatch.class);

	private Scheduler scheduler;
	private Vector<SchedulerIterator> schedulerIterators = new Vector<SchedulerIterator>();
	private SchedulerTask task;
	private int status;
	private int active;
	private final String name;
	private String msg;
	private String server;
	private final String host;
	
	private final String taskClassName;
	private boolean runOnce;

	DefaultBatch(String name, String host, String taskClassName) {
		this.taskClassName = taskClassName;
		this.name = name;
		this.host = host;
	}
	
	private void createTask() throws Exception {
		try {
			Class<?> taskClass = Class.forName(taskClassName);
			task = (SchedulerTask)taskClass.newInstance();
			task.setService(BatchControlImpl.getInstance());
			scheduler = new Scheduler();
		} catch (Exception e) {
			throw new Exception("Failed to create task: " + e);
		}
	}
	
	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}
	
	int getTaskState() {
		return task.getState();
	}

	public String getName() {
		return name;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public int getActive() {
		return active;
	}

	public void setActive(int active) {
		this.active = active;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public void addSchedulerIterator(SchedulerIterator iterator) {
		schedulerIterators.add(iterator);
		log.debug(name + ": iterator added " + iterator + ", size=" + schedulerIterators.size());
	}
	
	void stopTask() {
		try {
			MDC.put(SchedulerTask.LOGGER_KEY, "BATCH_STATUS");
			if(task == null) {
				return;
			}
			task.setStop(true);
			msg = "STOP: Task stopped " + task;
		} finally {
			log.info(msg);
			MDC.remove(SchedulerTask.LOGGER_KEY);
		}
	}

	void cancel() throws Exception {
		try {
			MDC.put(SchedulerTask.LOGGER_KEY, "BATCH_STATUS");
			if(task == null) {
				return;
			}
			task.cancel();
			scheduler.cancel();
			task = null;
			updateStatus(0);
			runOnce = false;
			msg = "CANCEL: Cancelled " + name;
		} catch (Exception e) {
			log.error("Failed to cancel batch: " + e.getMessage(), e);
			throw new Exception("Failed to cancel batch: " + e.getMessage());
		} finally {
			log.info(msg);
			MDC.remove(SchedulerTask.LOGGER_KEY);
		}
	}
	
	void setRunOnce(boolean runOnce) {
		this.runOnce = runOnce;
	}

	/**
	 * Starts the task if it is enabled (active=1) and sets
	 * status flag to 1. If task is already running (status=1)
	 * does nothing.
	 * 
	 * @throws Exception if task was not initialized.
	 */
	boolean start() throws Exception {
		boolean result = false;
		try {
			MDC.put(SchedulerTask.LOGGER_KEY, "BATCH_STATUS");
			if(active == 0) {
				msg = "START: NOT started " + name + ": not active.";
				updateStatus(0);
			} else if(server == null || !server.equals(host)) {
				msg = "START: NOT started " + name + ": server name '" + server + "' doesn't match "+host;
				updateStatus(0);
				updateActive(0);
			} else if(status == 1) {
				msg = "START: NOT started " + name + ": already scheduled.";
			} else {
				if(task == null) {
					createTask();
				}
				if(runOnce) {
					scheduler.scheduleOnce(task);
					updateStatus(1);
					task.setStop(false);
					result = true;
					msg = "START: started " + name + " for one-time execution.";
				} else {
					if(scheduler.schedule(task, getIterator())) {
						updateStatus(1);
						task.setStop(false);
						result = true;
						msg = "START: started " + name;
					} else {
						msg = "START: NOT started " + name + ": no schedule.";
					}
				}
			}
		} catch (Exception e) {
			msg = "START: NOT started " + name + ": "+e;
			log.error(e.toString(), e);
			throw e;
		} finally {
			log.info(msg);
			MDC.remove(SchedulerTask.LOGGER_KEY);
		}
		return result;
	}
	
	private void updateStatus(int status) throws Exception { 
		try {
			BatchControlImpl.getInstance().updateBatchStatus(name, status);
			setStatus(status);
		} catch (Exception e) {
			log.error("Failed to update " + name + " status", e);
			throw new Exception("Failed to update " + name + " status");
		}
	}

	private void updateActive(int active) throws Exception { 
		try {
			BatchControlImpl.getInstance().setActive(taskClassName, active);
		} catch (Exception e) {
			log.error("Failed to update " + name + " active status", e);
			throw new Exception("Failed to update " + name + " active status");
		}
	}
	
	private SchedulerIterator getIterator() {
		SchedulerIterator iterator = null;
		if(schedulerIterators.size()==1) {
			iterator = (SchedulerIterator)schedulerIterators.get(0);
			log.debug(name + ": returning single iterator: " + iterator);
		} else {
			iterator = new CompositeIterator(
					(SchedulerIterator[])schedulerIterators.toArray(new SchedulerIterator[0]));
			log.debug(name + ": returning composite iterator: " + iterator + ", size="+schedulerIterators.size());
		}
		return iterator;
	}
	
	public void clearIterators() {
		schedulerIterators.clear();
		log.debug(name + ": all iterators removed.");
	}
	
	public List<SchedulerIterator> getSchedulerIterators() {
		return schedulerIterators;
	}
	
	protected Object clone() {
		DefaultBatch copy = new DefaultBatch(this.name, this.host, this.taskClassName);
		copy.server = this.server;
		copy.active = this.active;
		copy.status = this.status;
		copy.runOnce = this.runOnce;
		copy.msg = this.msg;
		for(Iterator<SchedulerIterator> i = this.schedulerIterators.iterator(); i.hasNext(); ) {
			SchedulerIterator schedulerIterator = i.next();
			Object schedulerIteratorCopy;
			try {
				schedulerIteratorCopy = schedulerIterator.clone();
				copy.addSchedulerIterator((SchedulerIterator)schedulerIteratorCopy);
			} catch (CloneNotSupportedException e) {
				log.error("Clone not supported for " + schedulerIterator, e);
			}
		}
		return copy;
	}
	
}
