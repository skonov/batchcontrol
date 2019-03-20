package batchcontrol.services;

import java.util.Map;

public class BatchControlScheduleSupport implements BatchControl, ProcessListener {
	private final Object monitor = new Object();
	private final BatchControl subject;

	public BatchControlScheduleSupport(BatchControl subject) {
		this.subject = subject;
	}

	public synchronized void reloadBatches() throws Exception {
		synchronized (monitor) {
			subject.reloadBatches();
		}
	}

	public void stopBatch(String name) throws Exception {
		synchronized (monitor) {
			subject.stopBatch(name);
		}
	}

	public void processStopped() {
		synchronized (monitor) {
			subject.processStopped();
		}
	}

	public void startBatch(String taskClassName, boolean runOnce) throws Exception {
		synchronized (monitor) {
			subject.startBatch(taskClassName, runOnce);
		}
	}

	public synchronized void setActive(String batchClassName, int active) throws Exception {
		synchronized (monitor) {
			subject.setActive(batchClassName, active);
		}
	}

	public Map<String, DefaultBatch> getAllBatches() {
		synchronized (monitor) {
			return subject.getAllBatches();
		}
	}

	public DefaultBatch getBatch(String batchId) {
		synchronized (monitor) {
			return subject.getBatch(batchId);
		}
	}

	public void processStarted() {
		synchronized (monitor) {
			subject.processStarted();
		}
	}

	public void waitForBusyTask() throws Exception {
		synchronized (monitor) {
			monitor.wait();
		}
	}

	public void notifyProcessStopped() {
		synchronized (monitor) {
			monitor.notifyAll();
		}
	}

	public void startService() throws Exception {
		synchronized (monitor) {
			subject.startService();
		}
	}

	public void stopService() throws Exception {
		synchronized (monitor) {
			subject.stopService();
		}
	}

}
