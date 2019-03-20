package batchcontrol.service;

public interface ProcessListener {
	void waitForBusyTask() throws Exception;
	void notifyProcessStopped();
}
