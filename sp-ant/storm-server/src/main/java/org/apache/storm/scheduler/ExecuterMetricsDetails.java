package org.apache.storm.scheduler;

import java.util.Comparator;

public class ExecuterMetricsDetails  {


    public   String topologyID="";
    public   String superviorID="";
    public   String component="";

    public   Double executeLatency=0.0;
    public   Double capacity=0.0;
    public   String host="";
    public   int port=0;

    public   String workerSlot="";
   // public List<String> executers=null;
     public String executers="";







    public static Comparator<ExecuterMetricsDetails> capacityComparator = new Comparator<ExecuterMetricsDetails>() {

        public int compare(ExecuterMetricsDetails o1, ExecuterMetricsDetails o2) {

            return Double.compare(o1.getCapacity(), o2.getCapacity());

        }};


    public String getTopologyID() {
        return topologyID;
    }

    public String getComponent() {
        return component;
    }



    public String getWorkerSlot() {
        return workerSlot;
    }

    public Double getexecuteLatency() {
        return executeLatency;
    }

    public Double getCapacity() {
        return capacity;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getSuperviorID() {
        return superviorID;
    }

    public String getExecuters() {
        return executers;
    }

//    public List<String> getExecuters(){
//        return executers;
//    }

/////////////////////////////////////////////

    public  void setTopologyID( String topologyID ) {
         this.topologyID =topologyID;
    }

    public void setSuperviorID(String superviorID  ) {
        this.superviorID =superviorID;
    }

    public  void setWorkerSlot( String workerSlot ) {
         this.workerSlot =workerSlot;
    }

    public  void setComponent( String component ) {
        this.component =component;
    }

    public  void  setExecuteLatency( Double executeLatency ) {
         this.executeLatency =executeLatency;
    }

    public  Double  setCapacity( Double capacity ) {
        return this.capacity =capacity;
    }


    public  void setHost( String host ) {
         this.host =host;
    }

    public  void  setPort( int port ) {
         this.port =port;
    }


    public  void setExecuters( String executers ) {

        String startEndexecuter =executers;
        //System.out.println("..setExecuters. executers 1"+ executers  );

        String[] arrSplit = startEndexecuter.split("-");
        int startTask = Integer.parseInt( arrSplit[0]);
        int endTask = Integer.parseInt( arrSplit[1]);
        executers="";
        if (startTask == endTask )
        {
            executers ="["+startTask +", "+ endTask+"]";
        }else

        {

            for (int i=startTask ; i<= endTask ;i++)
            {
                //executers =  executers + "["+i +"-"+ i+"]";
                executers =  executers + "["+i +", "+ i+"]";
            }

        }


        this.executers =executers;
    }

//    public  void setExecuters( String executers ) {
//         this.executers.add(executers);
//    }




 ////////////////////////////////////////////////



   /* public List<Object> toList() {
        return Arrays.asList(CpuUsage, MemoryUsage);
    }
*/


}
