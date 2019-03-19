package batchcontrol.services;

import java.util.Map;

public interface BatchControl {
	public void reloadBatches() throws Exception;
	public void stopBatch(String name) throws Exception;
	public void processStopped();
	public void startBatch(String taskClassName, boolean runOnce) throws Exception;
	public void setActive(String batchClassName, int active) throws Exception;
	public Map<String, DefaultBatch> getAllBatches();
	public DefaultBatch getBatch(String batchId);
	public void processStarted();

}
