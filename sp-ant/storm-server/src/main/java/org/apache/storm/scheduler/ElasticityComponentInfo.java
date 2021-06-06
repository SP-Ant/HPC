package org.apache.storm.scheduler;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElasticityComponentInfo {

    public   String topologyID="";
    public   String topologyName="";
    public   String targetTopologyID="";
    public   String component="";
    public   String sourceSuperviorID="";
    public   String targetSuperviorID="";

    public   String sourceSuperviorName="";
    public   String targetSuperviorName="";



    public   WorkerSlot sourceWorkerSlot=null;
    public   WorkerSlot targetWorkerSlot=null;
    public   List<ExecutorDetails> executorToAssignOnTotal=null;


    public   Double  sourceCapacity=0.0;
    public   Double  targetWorkerNoneCpuLoad=0.0;
    public   Double  sourceWorkerNoneCpuLoad=0.0;

    public   Double  sourceTopologyCompleteLatencyBefore=0.0;
    public   Double  sourceTopologyCompleteLatencyAfter=0.0;
    public   Double  targetTopologyCompleteLatencyBefore=0.0;
    public   Double  targetTopologyCompleteLatencyAfter=0.0;

    public long startTime =0;
    public long endTime =0;

    public boolean returned =false;



//    public static Comparator<ExecuterMetricsDetails> capacityComparator = new Comparator<ExecuterMetricsDetails>() {
//
//        public int compare(ExecuterMetricsDetails o1, ExecuterMetricsDetails o2) {
//
//            return Double.compare(o1.getCapacity(), o2.getCapacity());
//
//        }};


    public String getTopologyID() {
        return topologyID;
    }

    public String getTargetTopologyID() {
        return targetTopologyID;
    }



    public String getComponent() {
        return component;
    }


    public String getsourceSuperviorID() {
        return sourceSuperviorID;
    }

    public String getTargetSuperviorID() {
        return targetSuperviorID;
    }


    public String getSourceSuperviorName() {
        return sourceSuperviorName;
    }


    public String getTargetSuperviorName() {
        return targetSuperviorName;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public Double getSourceTopologyCompleteLatencyBefore() {
        return sourceTopologyCompleteLatencyBefore;
    }

    public Double getSourceTopologyCompleteLatencyAfter() {
        return sourceTopologyCompleteLatencyAfter;
    }


    public Double getTargetTopologyCompleteLatencyBefore() {
        return targetTopologyCompleteLatencyBefore;
    }

    public Double getTargetTopologyCompleteLatencyAfter() {
        return targetTopologyCompleteLatencyAfter;
    }


    public WorkerSlot getSourceWorkerSlot() {
        return sourceWorkerSlot;
    }


    public WorkerSlot getTargetWorkerSlot() {
        return targetWorkerSlot;
    }

    public List<ExecutorDetails> getExecutorToAssignOnTotal() {
        return executorToAssignOnTotal;
    }


    public Double getSourceCapacity() {
        return sourceCapacity;
    }


    public Double getTargetWorkerNoneCpuLoad() {
        return targetWorkerNoneCpuLoad;
    }


    public Double getSourceWorkerNoneCpuLoad() {
        return sourceWorkerNoneCpuLoad;
    }


    public long  getStartTime () {
        return startTime;
    }

    public long  getEndTime () {
        return endTime ;
    }

    public boolean   getReturned  () {
        return returned  ;
    }



/////////////////////////////////////////////

    public  void setTopologyID( String topologyID ) {
        this.topologyID =topologyID;
    }

    public  void setTargetTopologyID( String targetTopologyID ) {
        this.targetTopologyID =targetTopologyID;
    }



    public void setsourceSuperviorID(String sourceSuperviorID  ) {
        this.sourceSuperviorID =sourceSuperviorID;
    }

    public void setTargetSuperviorIDSuperviorID(String targetSuperviorID  ) {
        this.targetSuperviorID =targetSuperviorID;
    }




    public void setSourceSuperviorName(String sourceSuperviorName  ) {

        sourceSuperviorName= extractIPfromSupervisorID(sourceSuperviorName);
        this.sourceSuperviorName =sourceSuperviorName;
    }



    public void setTargetSuperviorName(String targetSuperviorName  ) {
        targetSuperviorName= extractIPfromSupervisorID(targetSuperviorName);
        this.targetSuperviorName =targetSuperviorName;
    }



    public void setTopologyName(String topologyName  ) {
        this.topologyName =topologyName;
    }











    public  void setComponent( String component ) {
        this.component =component;
    }


    public  void setSourceWorkerSlot( WorkerSlot  sourceWorkerSlot ) {
        this.sourceWorkerSlot =sourceWorkerSlot;
    }

    public  void setExecutorToAssignOnTotal( List<ExecutorDetails> executorToAssignOnTotal ) {
        this.executorToAssignOnTotal =executorToAssignOnTotal;
    }




    public  void setTargetWorkerSlot( WorkerSlot  targetWorkerSlot ) {
        this.targetWorkerSlot =targetWorkerSlot;
    }

    public  Double  setSourceCapacity( Double sourceCapacity ) {
        return this.sourceCapacity =sourceCapacity;
    }

    public  Double  setTargetWorkerNoneCpuLoad( Double targetWorkerNoneCpuLoad ) {
        return this.targetWorkerNoneCpuLoad =targetWorkerNoneCpuLoad;
    }


    public  Double  setSourceWorkerNoneCpuLoad( Double sourceWorkerNoneCpuLoad ) {
        return this.sourceWorkerNoneCpuLoad =sourceWorkerNoneCpuLoad;
    }



    public  Double  setSourceTopologyCompleteLatencyBefore( Double sourceTopologyCompleteLatencyBefore ) {
        return this.sourceTopologyCompleteLatencyBefore =sourceTopologyCompleteLatencyBefore;
    }


    public  Double  setSourceTopologyCompleteLatencyAfter( Double sourceTopologyCompleteLatencyAfter ) {
        return this.sourceTopologyCompleteLatencyAfter =sourceTopologyCompleteLatencyAfter;
    }


    public  Double  setTargetTopologyCompleteLatencyBefore( Double targetTopologyCompleteLatencyBefore ) {
        return this.targetTopologyCompleteLatencyBefore =targetTopologyCompleteLatencyBefore;
    }

    public  Double  setTargetTopologyCompleteLatencyAfter( Double targetTopologyCompleteLatencyAfter ) {
        return this.targetTopologyCompleteLatencyAfter =targetTopologyCompleteLatencyAfter;
    }


    public  long  setStartTime ( long startTime ) {
        return this.startTime =startTime;
    }

    public  long  setEndTime ( long endTime ) {
        return this.endTime =endTime;
    }

    public  boolean  setReturned  ( boolean returned  ) {
        return this.returned   =returned ;
    }


    public   String extractIPfromSupervisorID(String SupervisorID) {

        String IPADDRESS_PATTERN =
                "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

        Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
        Matcher matcher = pattern.matcher(SupervisorID);
        if (matcher.find()) {
            return matcher.group();
            //System.out.println("*************Found**********"+ matcher.group().toString());
        } else {
            return "0.0.0.0";
            //System.out.println("*************Not Found*********"+ matcher.group().toString());
        }

    }


}
