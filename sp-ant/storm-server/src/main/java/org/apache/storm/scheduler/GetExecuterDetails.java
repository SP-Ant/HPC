package org.apache.storm.scheduler;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.NimbusClient;

import java.util.*;
import java.util.Map.Entry;

public class GetExecuterDetails  {

    private static final String WATCH_TRANSFERRED = "transferred";
    private static final String WATCH_EMITTED = "emitted";

    private int interval = 4;
    private String component;
    private static String stream;
    //private String watch;
    private static String watch;

    private static  Cluster cluster = null;
    private static  Map<Double, Double> resultMetric  =  new HashMap<>();
    private static  Map <String , Map<Double, Double> > resultMetricToComponenet  =  new HashMap<>();
    private static  Map <String , Map<Double, Double> > resultMetricSupervisor  =  new HashMap<>();
    private  static   Map <String , Map <String , Map<Double, Double>>> resultMetricTopology  =  new HashMap<>();
    public  static Map <String , Map <String , Map<Double, Double>>>  topologyCompponentsInfo  =  new HashMap<>();
    public static  Map<String, Object> resultGetComponentPage =  new HashMap<>();

    public static  List<String> executorsSpoutAcker =  new ArrayList<String>();
    public static Map<String, Object> config = ConfigUtils.readStormConfig();

    public static int topologyNumbers =0;
    public static Double valueTopologyExecuteTimeComplete =0.0;


    public static  Map <String , Map <String , Map<Double, Double>>> getExecuterDetailsDaemon() {


            DefaultScheduler defaultScheduler = new DefaultScheduler();
            cluster = defaultScheduler.getCluster();

            int cnt=0;

            for (TopologyDetails topology : cluster.getTopologies()) {
                if (!topology.getName().startsWith("BenchMarkTask") )
                {
                    cnt++;
                }
            }
            //System.out.println(":::::::::::::GetExecutersDetails:::::::::topologyNumbers::::::" + topologyNumbers);
            topologyNumbers = cnt;

            List<String> CandidtaeListForMigration = new ArrayList<String>();
            Map<String, SupervisorDetails> supervisordetls = cluster.getSupervisors();

            String id_supervisor = "";

            for (Map.Entry<String, SupervisorDetails> entry : supervisordetls.entrySet()) {
                id_supervisor = entry.getKey();
                //System.out.println("::::::::Thread:::::BenchMarkGetInfo:::::::::id_supervisor::::::" + id_supervisor);
            }

            String topologyId="";
            String topologyName="";

            resultMetricTopology.clear() ;
            for (TopologyDetails topology : cluster.getTopologies()) {
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException ex) {
//                    Thread.currentThread().interrupt();
//                }
                topologyId = topology.getId().toString();
                topologyName = topology.getName().toString();
                Topologies topologies = cluster.getTopologies();
                if (!topologyId.startsWith("BenchMarkTask")) {
                    Map <String , Map<Double, Double> > resultfromExecuters = new HashMap<>();
                    resultfromExecuters = getExecuterMetric(topologies, cluster, topologyId, topologyName);
                    String topoID=topologyId;
                    Double capacity =0.0 ;
                    Double executelatency=0.0;

                    Map <String , Map<Double, Double> > topologyinfo = new HashMap<>();
                    for (Entry  <String , Map<Double, Double> > entry : resultfromExecuters.entrySet()) {
                        String comp = entry.getKey();
                       // System.out.println(":::::::::::::GetExecuterDetails:::::::::comp::::::" + comp);
                        Map<Double, Double> info = entry.getValue();
                        for (Entry<Double,Double> entry2 : info.entrySet()) {
                            capacity =entry2.getKey();
                            executelatency =entry2.getValue();
                        }

                        String compname = comp;
                        Double execute=executelatency;
                        Double cap=capacity;
                        Map<Double, Double> tempvalues =  new HashMap<>();
                        tempvalues.put(cap,execute);
                        topologyinfo.put(compname,tempvalues );
                    }

                     resultMetricTopology.put(topoID, topologyinfo);

                    //System.out.println("---GetExecuterDetails----Final --resultMetricTopology-------" + "----" + resultMetricTopology.toString());

                }
            }


            if (resultMetricTopology.size()  == topologyNumbers )
            {
                topologyCompponentsInfo.putAll(resultMetricTopology);
                // System.out.println("::::::***:::::::GetExecuterDetails:::::::::topologyCompponentsInfo::::::" + topologyCompponentsInfo);
                //fastCheck=false;
            }

       // }





       return topologyCompponentsInfo;
    }



    public static Map<String, Object>  getComponentPage(String topologyID , String component)
    {


        //Map<String, Object> result;
        try {


            NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
                // Double result =0.0;
                @Override
                public void run(Nimbus.Iface nimbus) throws Exception {

                    String id="";
                    id = topologyID;
                    String comp=component;
                    String window=":all-time";
                    String user="ali";
                    boolean sys=false;
                    //Map
                    // public static Map<String, Object> config = ConfigUtils.readStormConfig();



                    resultGetComponentPage= MonitorScheduling.getComponentPage(nimbus,id,comp,window,sys,user,config);



                    // Nimbus.Iface client, String id, String component,
                    //       String window, boolean sys, String user, Map config)


                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return resultGetComponentPage;


    }







    public static Double  getTopologyExecuteCompleteTime(String topologyID)
    {

      //Double finalValue =1.0;
      // final  Double result =0.0;

        try {


            NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
               // Double result =0.0;
                @Override
                public void run(Nimbus.Iface nimbus) throws Exception {
                Map<String, Map<String, Double>> executecomplete = MonitorScheduling.getTopologyExecuteCompleteTime(nimbus, topologyID);

                   if (executecomplete.size() == 0)
                   {
                       valueTopologyExecuteTimeComplete =0.0;
                      // valueTopologyExecuteTimeComplete =null;
                       return ;
                       //return valueTopologyExecuteTimeComplete;

                   }

                    for (Entry  <String, Map<String, Double>>  entry : executecomplete.entrySet()) {
                        String topologyId = entry.getKey();
                        // System.out.println(":::::::::::::GetExecuterDetails:::::::::comp::::::" + comp);
                        Map<String, Double> info = entry.getValue();
                        for (Entry<String, Double> entry2 : info.entrySet()) {
                            String stream = entry2.getKey();
                            Double value = entry2.getValue();
                            //GetExecuterDetails.newfunction().re
                            valueTopologyExecuteTimeComplete = value;

                       }

                    }

                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }


     return valueTopologyExecuteTimeComplete;

    }




    public static List<String>  getSpoutAckerExecuters(String topologyID)
    {

        //Double finalValue =1.0;
        // final  Double result =0.0;
        executorsSpoutAcker.clear() ;

        try {


            NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
                // Double result =0.0;
                @Override
                public void run(Nimbus.Iface nimbus) throws Exception {
                     executorsSpoutAcker = MonitorScheduling.getTopologySpoutAckerExecuters(nimbus, topologyID);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return executorsSpoutAcker;

    }




    public static  Map <String , Map<Double, Double> > getExecuterMetric(Topologies topologies, Cluster cluster, String topologyID, String topologyName) {


        Collection<WorkerSlot> workerslot =cluster.getUsedSlots();
        int num_worker = workerslot.size();
        MonitorScheduling monitor =new MonitorScheduling();
        if (num_worker > 0) {

//            try {
//                Thread.sleep(10000);
//            } catch (InterruptedException ex) {
//                Thread.currentThread().interrupt();
//            }

            Integer interval = 4;
            String component = "";

            try {

                NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
                    @Override
                    public void run(Nimbus.Iface nimbus) throws Exception {

                        if ( num_worker == 0)  return;
                        HashSet<String> components = monitor.getComponents(nimbus, topologyName);
                       // getTopologyExecuteCompleteTime
                        //monitor.getTopologyExecuteCompleteTime(nimbus, topologyName);

                        MonitorScheduling.clearMetricToComponent();/// clear the map
                        for (String comp : components) {
                            if (!comp.startsWith("__acker")  && !comp.startsWith("log") ) {
                         //  if (!comp.startsWith("__acker")  ) {
                                MonitorScheduling monitor =new MonitorScheduling();
                                // monitor.setComponent(comp);
                                String stream = "default";
                                String watch = "emitted";
                               // String watch = "transferred";
                                //String topologyName = "MyTopology";
                                monitor.setComponent(comp);
                                monitor.setStream(stream);
                                monitor.setWatch(watch);
                                monitor.setTopologyID(topologyID);
                                monitor.setTopology(topologyID);
                                resultMetric= monitor.metrics(nimbus);
                            }
                        }
                        resultMetricToComponenet = MonitorScheduling.getMetricToComponent();
                        //System.out.println("----------GetExecuterDetails-----resultMetricComponenet---"+ resultMetricToComponenet );
                        //MonitorScheduling.clearMetricToComponent();/// clear the map
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return resultMetricToComponenet ;
    }







}
