package org.apache.storm.scheduler;

public class AugExecutorDetails {

    private ExecutorDetails executor;
    private String componentId;
    private WorkerSlot workerSlot;

    public AugExecutorDetails(ExecutorDetails exec, String componentId, WorkerSlot workerSlot)
    {
        this.executor = exec;
        this.componentId = componentId;
        this.workerSlot = workerSlot;
    }

    public AugExecutorDetails(ExecutorDetails exec, TopologyDetails topology, SchedulerAssignment assignment)
    {
        this.executor = exec;
        this.componentId = topology.getExecutorToComponent().get(exec);
        this.workerSlot = assignment.getExecutorToSlot().get(exec);
    }

    public ExecutorDetails getExecutor() {
        return executor;
    }

    public String getComponentId() {
        return componentId;
    }

    public WorkerSlot getWorkerSlot() {
        return workerSlot;
    }

    public String getNodeId() {
        return (workerSlot != null ? workerSlot.getNodeId() : null);
    }
}
