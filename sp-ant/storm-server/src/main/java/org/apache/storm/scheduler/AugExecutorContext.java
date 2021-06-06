package org.apache.storm.scheduler;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.GeneralTopologyContext;

import java.util.*;

public class AugExecutorContext {

    private static final String  SYSTEM_COMPONENT_PREFIX = "__";

    private AugExecutorDetails augExecutor;
    List<AugExecutorDetails> neighborExecutors = new ArrayList<AugExecutorDetails>();
    Map<ExecutorDetails, Boolean> isTargetMapping = new HashMap<ExecutorDetails, Boolean>();


    public AugExecutorContext(AugExecutorDetails augExecutor)
    {
        this.augExecutor = augExecutor;
    }

    public AugExecutorDetails getAugExecutor() {
        return augExecutor;
    }


    private List<String> extractSourceComponentsId(String componentId, GeneralTopologyContext context)
    {
        List<String> sourceComponentsId = new ArrayList<String>();

        Map<GlobalStreamId, Grouping> sources = context.getSources(componentId);
        for(GlobalStreamId globalStreamID : sources.keySet()){
            if (globalStreamID!=null && !globalStreamID.get_componentId().startsWith(SYSTEM_COMPONENT_PREFIX))
                sourceComponentsId.add(globalStreamID.get_componentId());
        }

        return sourceComponentsId;
    }

    private List<String> extractTargetComponentsId(String componentId, GeneralTopologyContext context)
    {
        List<String> targetComponentsId = new ArrayList<String>();

        Map<String, Map<String, Grouping>> targets = context.getTargets(componentId);

        for(String streamId : targets.keySet()){
            Set<String> componentsId = targets.get(streamId).keySet();

            if (streamId!=null && streamId.equals("default"))
                targetComponentsId.addAll(componentsId);
        }

        return targetComponentsId;
    }


    public List<AugExecutorDetails> getNeighborExecutors() {
        return neighborExecutors;
    }

    public Boolean isTarget(AugExecutorDetails exec)
    {
        return isTargetMapping.get(exec.getExecutor());
    }




}
