package batchcontrol.services;

public interface ProcessListener {
	void waitForBusyTask() throws Exception;
	void notifyProcessStopped();
}
