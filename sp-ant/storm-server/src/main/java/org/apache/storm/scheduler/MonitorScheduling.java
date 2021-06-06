package org.apache.storm.scheduler;


import com.google.common.base.Joiner;
import org.apache.storm.Constants;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.*;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static org.apache.storm.utils.Utils.nullToZero;


public class MonitorScheduling {

    private static final String WATCH_TRANSFERRED = "transferred";
    private static final String WATCH_EMITTED = "emitted";

    private int interval = 4;
    private String topology;
    private String topologyID;
    private String component;
    private String stream;
    private String watch;
    private  Map<Double, Double> result =  new HashMap<>();
    private  Map<Double, Double> newresult =  new HashMap<>();
    private  Map<Double, Double> newresultBenchmark =  new HashMap<>();
    private static  Map <String , Map<Double, Double> > resultMetricToComponent =  new HashMap<>();
    private static   Map<Double, Double> resultMetricToComponentBenchmark =  new HashMap<>();

    private static final Object[][] PRETTY_SEC_DIVIDERS = {
            new Object[]{ "s", 60 },
            new Object[]{ "m", 60 },
            new Object[]{ "h", 24 },
            new Object[]{ "d", null }
    };


//    private  Map<Double, Double> newresultmap =  new HashMap<Double, Double>();



    public static void call_monitor(){


    }




    public static Map<String, Object> getComponentPage(Nimbus.Iface client, String id, String component,
                                                       //String window, boolean sys, String user) throws org.apache.thrift.TException {
                                                       String window, boolean sys, String user, Map config) throws Exception {
        Map<String, Object> result = new HashMap();
        // System.out.println(".........getComponentPage..1..  "   );
        ComponentPageInfo componentPageInfo = client.getComponentPageInfo(
                id, component, window, sys
        );

//        System.out.println(".........topology id....  " +id  );
//        System.out.println("........ window..  " +window  );
//        System.out.println(".........user..  " +id  );
//        System.out.println(".........component..  " +component  );
        //System.out.println(".........M id..2..  " +id  );

        // System.out.println(".........getComponentPage..2..  "   );
        if (componentPageInfo.get_component_type().equals(ComponentType.BOLT)) {
            result.putAll(unpackBoltPageInfo(componentPageInfo, id, window, sys, config));
            //   System.out.println(".........getComponentPage..3..  "   );
        } else if ((componentPageInfo.get_component_type().equals(ComponentType.SPOUT))) {
            result.putAll(unpackSpoutPageInfo(componentPageInfo, id, window, sys, config));
            // System.out.println(".........getComponentPage..4..  "   );
        }

        result.put("user", user);
        result.put("id" , component);
        result.put("encodedId", Utils.urlEncodeUtf8(component));
        result.put("name", componentPageInfo.get_topology_name());
        result.put("executors", componentPageInfo.get_num_executors());
        result.put("tasks", componentPageInfo.get_num_tasks());
        result.put("requestedMemOnHeap",
                componentPageInfo.get_resources_map().get(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME));
        result.put("requestedMemOffHeap",
                componentPageInfo.get_resources_map().get(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME));
        result.put("requestedCpu",
                componentPageInfo.get_resources_map().get(Constants.COMMON_CPU_RESOURCE_NAME));

        result.put("schedulerDisplayResource", config.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));
        result.put("topologyId", id);
        result.put("topologyStatus", componentPageInfo.get_topology_status());
        result.put("encodedTopologyId", Utils.urlEncodeUtf8(id));
        result.put("window", window);
        result.put("componentType", componentPageInfo.get_component_type().toString().toLowerCase());
        result.put("windowHint", getWindowHint(window));
        result.put("debug", componentPageInfo.is_set_debug_options() && componentPageInfo.get_debug_options().is_enable());
        double samplingPct = 10;
        if (componentPageInfo.is_set_debug_options()) {
            samplingPct = componentPageInfo.get_debug_options().get_samplingpct();
        }
        result.put("samplingPct", samplingPct);
        String eventlogHost = componentPageInfo.get_eventlog_host();
        if (null != eventlogHost && !eventlogHost.isEmpty()) {
//            result.put("eventLogLink", getLogviewerLink(eventlogHost,
//                    WebAppUtils.eventLogsFilename(id, String.valueOf(componentPageInfo.get_eventlog_port())),
//                    config, componentPageInfo.get_eventlog_port()));
        }
        result.put("profilingAndDebuggingCapable", !Utils.isOnWindows());
        result.put("profileActionEnabled", config.get(DaemonConfig.WORKER_PROFILER_ENABLED));


        //result.put("profilerActive", getActiveProfileActions(client, id, component, config));

        // System.out.println(".........result..  " +result  );


//        Double totalCpu =0.0;
//        Double totalMem =0.0;
        for (Entry <String,Object> entry : result.entrySet()) {
            String str="";
            Object obj=null;
            str = entry.getKey();
            obj =entry.getValue();



        }



        return result;
    }


    public static String getWindowHint(String window) {
        if (window.equals(":all-time")) {
            return "All time";
        }
        return prettyUptimeSec(window);
    }


    public static Map<String, Object> unpackBoltPageInfo(ComponentPageInfo componentPageInfo,
                                                         String topologyId, String window, boolean sys,
                                                         Map config) {
        Map<String, Object> result = new HashMap<>();

        result.put(
                "boltStats",
                componentPageInfo.get_window_to_stats().entrySet().stream().map(
                        e -> getBoltAggStatsMap(e.getValue(), e.getKey())
                ).collect(Collectors.toList())
        );
        result.put(
                "inputStats",
                componentPageInfo.get_gsid_to_input_stats().entrySet().stream().map(
                        e -> getBoltInputStats(e.getKey(), e.getValue())
                ).collect(Collectors.toList())
        );
        result.put(
                "outputStats",
                componentPageInfo.get_sid_to_output_stats().entrySet().stream().map(
                        e -> getBoltOutputStats(e.getKey(), e.getValue())
                ).collect(Collectors.toList())
        );
        result.put(
                "executorStats",
                componentPageInfo.get_exec_stats().stream().map(
                        e -> getBoltExecutorStats(topologyId, config, e)
                ).collect(Collectors.toList())
        );
        result.putAll(getComponentErrors(componentPageInfo.get_errors(), topologyId, config));
        return result;
    }






    public static Map<String, Object> unpackSpoutPageInfo(ComponentPageInfo componentPageInfo,
                                                          String topologyId, String window, boolean sys,
                                                          Map config) {
        Map<String, Object> result = new HashMap<>();
        result.put(
                "spoutSummary",
                componentPageInfo.get_window_to_stats().entrySet().stream().map(
                        e -> getSpoutAggStatsMap(e.getValue(), e.getKey())
                ).collect(Collectors.toList())
        );
        result.put(
                "outputStats",
                componentPageInfo.get_sid_to_output_stats().entrySet().stream().map(
                        e -> getSpoutOutputStats(e.getKey(), e.getValue())
                ).collect(Collectors.toList())
        );
        result.put(
                "executorStats",
                componentPageInfo.get_exec_stats().stream().map(
                        e -> getSpoutExecutorStats(topologyId, config, e)
                ).collect(Collectors.toList())
        );
        result.putAll(getComponentErrors(componentPageInfo.get_errors(), topologyId, config));
        return result;
    }

    private static Map<String, Object> getSpoutExecutorStats(String topologyId, Map<String, Object> config,
                                                             ExecutorAggregateStats executorAggregateStats) {
        Map<String, Object> result = new HashMap();
        ExecutorSummary executorSummary = executorAggregateStats.get_exec_summary();
        ExecutorInfo executorInfo = executorSummary.get_executor_info();
        ComponentAggregateStats componentAggregateStats = executorAggregateStats.get_stats();
        SpecificAggregateStats specificAggregateStats = componentAggregateStats.get_specific_stats();
        SpoutAggregateStats spoutAggregateStats = specificAggregateStats.get_spout();
        CommonAggregateStats commonAggregateStats = componentAggregateStats.get_common_stats();
        String executorId = prettyExecutorInfo(executorInfo);
        result.put("id", executorId);
        result.put("encodedId", Utils.urlEncodeUtf8(executorId));
        result.put("uptime", prettyUptimeSec(executorSummary.get_uptime_secs()));
        result.put("uptimeSeconds", executorSummary.get_uptime_secs());
        String host = executorSummary.get_host();
        result.put("host", host);
        int port = executorSummary.get_port();
        result.put("port", port);
        result.put("emitted", nullToZero((double) commonAggregateStats.get_emitted()));
        result.put("transferred", nullToZero((double) commonAggregateStats.get_transferred()));
        result.put("completeLatency", StatsUtil.floatStr(spoutAggregateStats.get_complete_latency_ms()));
        result.put("acked", nullToZero((double) commonAggregateStats.get_acked()));
        result.put("failed", nullToZero((double) commonAggregateStats.get_failed()));
        result.put("workerLogLink", getWorkerLogLink(host, port, config, topologyId));
        return result;
    }



    private static Map<String, Object> getSpoutOutputStats(String streamId,
                                                           ComponentAggregateStats componentAggregateStats) {
        SpecificAggregateStats specificAggregateStats = componentAggregateStats.get_specific_stats();
        SpoutAggregateStats spoutAggregateStats = specificAggregateStats.get_spout();
        Map<String, Object> result = new HashMap();
        result.put("stream", streamId);
        CommonAggregateStats commonStats = componentAggregateStats.get_common_stats();
        result.put("emitted", nullToZero((double) commonStats.get_emitted()));
        result.put("transferred", nullToZero((double) commonStats.get_transferred()));
        result.put("completeLatency", StatsUtil.floatStr(spoutAggregateStats.get_complete_latency_ms()));
        result.put("acked", nullToZero((double) commonStats.get_acked()));
        result.put("failed", nullToZero((double) commonStats.get_failed()));
        return result;
    }


    private static Map<String, Object> getSpoutAggStatsMap(
            ComponentAggregateStats componentAggregateStats, String window) {
        Map<String, Object> result = new HashMap();
        SpoutAggregateStats spoutAggregateStats = componentAggregateStats.get_specific_stats().get_spout();
        CommonAggregateStats commonStats = componentAggregateStats.get_common_stats();
        result.put("window", window);
        result.put("windowPretty", getWindowHint(window));
        result.put("emitted", commonStats.get_emitted());
        result.put("transferred", commonStats.get_transferred());
        result.put("acked", commonStats.get_acked());
        result.put("failed", commonStats.get_failed());
        result.put("completeLatency", spoutAggregateStats.get_complete_latency_ms());


        ErrorInfo lastError = componentAggregateStats.get_last_error();
        result.put("lastError", Objects.isNull(lastError) ?  "" : getTruncatedErrorString(lastError.get_error()));
        return result;
    }

    private static String getTruncatedErrorString(String errorString) {
        return errorString.substring(0, Math.min(errorString.length(), 200));
    }



    private static Map<String, Object> getBoltExecutorStats(String topologyId, Map<String, Object> config,
                                                            ExecutorAggregateStats executorAggregateStats) {
        Map<String, Object> result = new HashMap();
        ExecutorSummary executorSummary = executorAggregateStats.get_exec_summary();
        ExecutorInfo executorInfo = executorSummary.get_executor_info();
        String executorId = prettyExecutorInfo(executorInfo);
        result.put("id", executorId);
        result.put("encodedId", Utils.urlEncodeUtf8(executorId));
        result.put("uptime", prettyUptimeSec(executorSummary.get_uptime_secs()));
        result.put("uptimeSeconds", executorSummary.get_uptime_secs());
        String host = executorSummary.get_host();
        result.put("host", host);
        int port = executorSummary.get_port();
        result.put("port", port);

        ComponentAggregateStats componentAggregateStats = executorAggregateStats.get_stats();
        CommonAggregateStats commonAggregateStats = componentAggregateStats.get_common_stats();
        result.put("emitted", nullToZero((double) commonAggregateStats.get_emitted()));
        result.put("transferred", nullToZero((double) commonAggregateStats.get_transferred()));

        SpecificAggregateStats specificAggregateStats = componentAggregateStats.get_specific_stats();
        BoltAggregateStats boltAggregateStats = specificAggregateStats.get_bolt();
        result.put("capacity",  StatsUtil.floatStr(nullToZero(boltAggregateStats.get_capacity())));
        result.put("executeLatency", StatsUtil.floatStr(boltAggregateStats.get_execute_latency_ms()));
        result.put("executed", nullToZero((double) boltAggregateStats.get_executed()));
        result.put("processLatency", StatsUtil.floatStr(boltAggregateStats.get_process_latency_ms()));
        result.put("acked", nullToZero((double) commonAggregateStats.get_acked()));
        result.put("failed", nullToZero((double) commonAggregateStats.get_failed()));
        result.put("workerLogLink", getWorkerLogLink(host, port, config, topologyId));
        return result;
    }


    private static Map<String, Object> getBoltOutputStats(String streamId,
                                                          ComponentAggregateStats componentAggregateStats) {
        Map<String, Object> result = new HashMap();
        result.put("stream", streamId);
        CommonAggregateStats commonStats = componentAggregateStats.get_common_stats();
        result.put("emitted", nullToZero((double) commonStats.get_emitted()));
        result.put("transferred", nullToZero((double) commonStats.get_transferred()));
        return result;
    }

    public static String prettyExecutorInfo(ExecutorInfo e) {
        return "[" + e.get_task_start() + "-" + e.get_task_end() + "]";
    }

    public static String prettyUptimeSec(int secs) {
        return prettyUptimeStr(String.valueOf(secs), PRETTY_SEC_DIVIDERS);
    }

    public static String prettyUptimeSec(String sec) {
        return prettyUptimeStr(sec, PRETTY_SEC_DIVIDERS);
    }



    public static String prettyUptimeStr(String val, Object[][] dividers) {
        int uptime = Integer.parseInt(val);
        LinkedList<String> tmp = new LinkedList<>();
        for (Object[] divider : dividers) {
            if (uptime > 0) {
                String state = (String) divider[0];
                Integer div = (Integer) divider[1];
                if (div != null) {
                    tmp.addFirst(uptime % div + state);
                    uptime = uptime / div;
                } else {
                    tmp.addFirst(uptime + state);
                }
            }
        }
        return Joiner.on(" ").join(tmp);
    }


    public static String getWorkerLogLink(String host, int port,
                                          Map<String, Object> config, String topologyId) {
        return getLogviewerLink(host,
                //WebAppUtils.logsFilename(
                logsFilename(
                        topologyId, String.valueOf(port)),
                config, port
        );
    }

    public static String logsFilename(String stormId, String port) {
        return stormId + File.separator + port + File.separator + "worker.log";
    }


    public static boolean isSecureLogviewer(Map<String, Object> config) {
        if (config.containsKey(DaemonConfig.LOGVIEWER_HTTPS_PORT)) {
            int logviewerPort = (int) config.get(DaemonConfig.LOGVIEWER_HTTPS_PORT);
            if (logviewerPort >= 0) {
                return true;
            }
        }
        return false;
    }


    public static String getLogviewerLink(String host, String fname,
                                          Map<String, Object> config, int port) {
        String nullstr="";
        if (isSecureLogviewer(config)) {

            return nullstr;
            // return UIHelpers.urlFormat("https://%s:%s/api/v1/log?file=%s",
            // return nullstr,
            //        host, config.get(DaemonConfig.LOGVIEWER_HTTPS_PORT), fname);


        } else {
            //return UIHelpers.urlFormat("http://%s:%s/api/v1/log?file=%s",
            //   host, config.get(DaemonConfig.LOGVIEWER_PORT), fname);
            return nullstr;
        }
    }


    private static Map<String, Object> getBoltAggStatsMap(
            ComponentAggregateStats componentAggregateStats, String window) {
        Map<String, Object> result = new HashMap();
        CommonAggregateStats commonStats = componentAggregateStats.get_common_stats();
        // System.out.println(".........getBoltAggStatsMap......window....  " );
        result.put("window", window);
        result.put("windowPretty", getWindowHint(window));
        result.put("emitted", commonStats.get_emitted());
        result.put("transferred", commonStats.get_transferred());
        result.put("acked", commonStats.get_acked());
        result.put("failed", commonStats.get_failed());
        BoltAggregateStats boltAggregateStats = componentAggregateStats.get_specific_stats().get_bolt();
        result.put("executeLatency", StatsUtil.floatStr(boltAggregateStats.get_execute_latency_ms()));
        result.put("executed", boltAggregateStats.get_executed());
        result.put("processLatency", StatsUtil.floatStr(boltAggregateStats.get_process_latency_ms()));
        result.put("capacity", StatsUtil.floatStr(boltAggregateStats.get_capacity()));

        //System.out.println(".........getBoltAggStatsMap......result....  " + result );
        return result;
    }

    private static Map<String, Object> getBoltInputStats(GlobalStreamId globalStreamId,
                                                         ComponentAggregateStats componentAggregateStats) {
        Map<String, Object> result = new HashMap();
        SpecificAggregateStats specificAggregateStats = componentAggregateStats.get_specific_stats();
        BoltAggregateStats boltAggregateStats = specificAggregateStats.get_bolt();
        CommonAggregateStats commonAggregateStats = componentAggregateStats.get_common_stats();
        String componentId = globalStreamId.get_componentId();
        result.put("component", componentId);
        result.put("encodedComponentId", Utils.urlEncodeUtf8(componentId));
        result.put("stream", globalStreamId.get_streamId());
        result.put("executeLatency", StatsUtil.floatStr(boltAggregateStats.get_execute_latency_ms()));
        result.put("processLatency", StatsUtil.floatStr(boltAggregateStats.get_process_latency_ms()));
        result.put("executed", nullToZero((double) boltAggregateStats.get_executed()));
        result.put("acked", nullToZero((double) commonAggregateStats.get_acked()));
        result.put("failed", nullToZero((double) commonAggregateStats.get_failed()));
        return result;
    }


    private static Map<String, Object> getComponentErrors(List<ErrorInfo> errorInfoList,
                                                          String topologyId, Map config) {
        Map<String, Object> result = new HashMap();
        errorInfoList.sort(Comparator.comparingInt(ErrorInfo::get_error_time_secs));
        result.put(
                "componentErrors",
                errorInfoList.stream().map(e -> getComponentErrorInfo(e, config, topologyId))
                        .collect(Collectors.toList())
        );
        return result;
    }

    private static Map<String, Object> getComponentErrorInfo(ErrorInfo errorInfo, Map config,
                                                             String topologyId) {
        Map<String, Object> result = new HashMap();
        result.put("errorTime",
                errorInfo.get_error_time_secs());
        String host = errorInfo.get_host();
        result.put("errorHost", host);
        int port = errorInfo.get_port();
        result.put("errorPort", port);
        result.put("errorWorkerLogLink", getWorkerLogLink(host, port, config, topologyId));
        result.put("errorLapsedSecs", Time.deltaSecs(errorInfo.get_error_time_secs()));
        result.put("error", errorInfo.get_error());
        return result;
    }





////////////////////////////////////////////////////////////////////



    public static  List<String> getTopologySpoutAckerExecuters(Nimbus.Iface client, String topology) throws Exception {
        List<String> executorsSpoutAcker = new ArrayList<String>();
        HashSet<String> components = new HashSet<>();
        Map<String, Map<String, Double>> completeTime = new HashMap<>();
        ClusterSummary clusterSummary = client.getClusterInfo();
        TopologySummary topologySummary = null;
        for (TopologySummary ts : clusterSummary.get_topologies()) {
            if (topology.equals(ts.get_id())) {
                topologySummary = ts;
                break;
            }
        }
        if (topologySummary == null) {
            throw new IllegalArgumentException("topology: " + topology + " not found");
        } else {
            String id = topologySummary.get_id();
            GetInfoOptions getInfoOpts = new GetInfoOptions();
            getInfoOpts.set_num_err_choice(NumErrorsChoice.NONE);
            TopologyInfo info = client.getTopologyInfoWithOpts(id, getInfoOpts);

            for (ExecutorSummary es : info.get_executors()) {

                if (es.get_component_id().equals("sentenceGenerator")  || es.get_component_id().startsWith("__acker")     ) {
                  //  System.out.println(".......getComponentsToSupervisor ......get_component_id:.." + es.get_component_id() + ".." + es.get_executor_info().get_task_start() + ".." + es.get_executor_info().get_task_end());
                int startTask = es.get_executor_info().get_task_start();
                int endTask = es.get_executor_info().get_task_end();
                String executers="";
                if (startTask == endTask )
                {
                    executers ="["+startTask +", "+ endTask+"]";
                    executorsSpoutAcker.add(executers);
                }else
                {
                    for (int i=startTask ; i<= endTask ;i++)
                    {
                        executers =  executers + "["+i +", "+ i+"]";
                        executorsSpoutAcker.add(executers);
                    }

                }


                }
            }
            return executorsSpoutAcker;
        }
       // return executorsSpoutAcker;
    }








    public static  Map<String, Map<String, Double>> getTopologyExecuteCompleteTime(Nimbus.Iface client, String topology) throws Exception {
        HashSet<String> components = new HashSet<>();
        Map<String, Map<String, Double>> completeTime = new HashMap<>();
        ClusterSummary clusterSummary = client.getClusterInfo();
        TopologySummary topologySummary = null;
        for (TopologySummary ts : clusterSummary.get_topologies()) {
            if (topology.equals(ts.get_id())) {
                topologySummary = ts;
                  break;
            }
        }
        if (topologySummary == null) {
            throw new IllegalArgumentException("topology: " + topology + " not found");
        } else {
            String id = topologySummary.get_id();
            GetInfoOptions getInfoOpts = new GetInfoOptions();
            getInfoOpts.set_num_err_choice(NumErrorsChoice.NONE);
            TopologyInfo info = client.getTopologyInfoWithOpts(id, getInfoOpts);
            for (ExecutorSummary es : info.get_executors()) {

//es.get
              if (  es.get_stats().get_specific().is_set_spout())
              {
                  completeTime = es.get_stats().get_specific().get_spout().get_complete_ms_avg() ;
                 // System.out.println(".......getComponentsToSupervisor ......completeTime:.." + completeTime);

                 // int numberexecuted = es.get_stats().get_specific().get_bolt().get_executed_size() ;
                 // System.out.println(".......getComponentsToSupervisor ......numberexecuted:.." + numberexecuted);


              }
            }
        }
        return completeTime;
    }







    public HashSet<String> getComponents(Nimbus.Iface client, String topology) throws Exception {
        HashSet<String> components = new HashSet<>();
        ClusterSummary clusterSummary = client.getClusterInfo();
        TopologySummary topologySummary = null;
        for (TopologySummary ts : clusterSummary.get_topologies()) {
            if (topology.equals(ts.get_name())) {
                topologySummary = ts;
                //ts.port
                break;
            }
        }
        if (topologySummary == null) {
            throw new IllegalArgumentException("topology: " + topology + " not found");
        } else {
            String id = topologySummary.get_id();
            GetInfoOptions getInfoOpts = new GetInfoOptions();
            getInfoOpts.set_num_err_choice(NumErrorsChoice.NONE);
            TopologyInfo info = client.getTopologyInfoWithOpts(id, getInfoOpts);

            for (ExecutorSummary es : info.get_executors()) {

                components.add(es.get_component_id());

            }
        }
        return components;
    }



    public  Map<Double, Double> metrics(Nimbus.Iface client ) throws Exception {

        //topology =topologyID;
        if (interval <= 0) {
            throw new IllegalArgumentException("poll interval must be positive");
        }

        if (topology == null || topology.isEmpty()) {
            throw new IllegalArgumentException("topology name must be something");
        }

        if (component == null || component.isEmpty()) {
            HashSet<String> components = getComponents(client, topology);
            System.out.println("Available components for " + topology + " :");
            System.out.println("------------------");
            for (String comp : components) {
                System.out.println(comp);
            }
            System.out.println("------------------");
            System.out.println("Please use -m to specify one component");
            return result;
        }
        if (stream == null || stream.isEmpty()) {
            throw new IllegalArgumentException("stream name must be something");
        }

        if (!WATCH_TRANSFERRED.equals(watch) && !WATCH_EMITTED.equals(watch)) {
            throw new IllegalArgumentException("watch item must either be transferred or emitted");
        }
        //System.out.println("topology\tcomponent\tparallelism\tstream\ttime-diff ms\t" + watch + "\tthroughput (Kt/s)");


        long pollMs = interval * 1000;
        long now = System.currentTimeMillis();
        MonitorScheduling.MetricsState state = new MonitorScheduling.MetricsState(now, 0);
        MonitorScheduling.Poller poller = new MonitorScheduling.Poller(now, pollMs);


        do {
            metrics(client, now, state);
            /// hd
            //break;
            /// hd
            try {
                now = poller.nextPoll();
            } catch (InterruptedException e) {
            e.printStackTrace();
             //hmd
             //break;
             return  result ;
             //hmd
            }
            break;
        } while (true);

        return  result ;
    }

    public void metrics(Nimbus.Iface client, long now, MonitorScheduling.MetricsState state) throws Exception {
        long totalStatted = 0;

        int componentParallelism = 0;
        boolean streamFound = false;

        ClusterSummary clusterSummary = client.getClusterInfo();
       // System.out.println("+++++++++clusterSummary::::::stats::::: "+  clusterSummary.toString() );

        TopologySummary topologySummary = null;
        for (TopologySummary ts : clusterSummary.get_topologies()) {
            //System.out.println("+++++++++ts::::::000::::: "+ ts.get_id());
           // System.out.println("+++++++++ts::::::111::::: "+ topologyID);
            if (topologyID.equals(ts.get_id())) {
               // System.out.println("+++++++++ts::::::222::::: "+ ts.get_id());
               // System.out.println("+++++++++ts::::::333::::: "+ topologyID);


                topologySummary = ts;
                break;
            }
        }
        if (topologySummary == null) {
            throw new IllegalArgumentException("topologyID: " + topologyID + " not found");
        } else {
            //System.out.println("+++++++++ts::::::444::::: "+ topologyID);
            String id = topologySummary.get_id();
            GetInfoOptions getInfoOpts = new GetInfoOptions();
            getInfoOpts.set_num_err_choice(NumErrorsChoice.NONE);
            TopologyInfo info = client.getTopologyInfoWithOpts(id, getInfoOpts);

            for (ExecutorSummary es : info.get_executors()) {

                if (component.equals(es.get_component_id())) {
                    componentParallelism++;
                    ExecutorStats stats = es.get_stats();
                    if (stats != null) {
                        //System.out.println("::::::::::Going to cll  105::::::stats::::: "+  stats.toString() );

                        Map<String, Map<String, Long>> statted =
                                WATCH_EMITTED.equals(watch) ? stats.get_emitted() : stats.get_transferred();

                        if (statted != null) {
                           // System.out.println("::::::::::statted::::: "+ statted.toString() );
                           // System.out.println("##################Going to cll  105::::::get_emitted::::: "+ stats.get_emitted().toString());
                            double rate= stats.get_rate();
                            String host = es.get_host() ;
                            int port = es.get_port() ;
                            int startTaskExecuters = es.get_executor_info().get_task_start() ;
                            int endTaskExecuters = es.get_executor_info().get_task_end() ;


                            //System.out.println("::::::::::es::::: "+component+":::"+ es.get_executor_info() );
                            //System.out.println("::::::::::es::::: "+ es );

                            //System.out.println("::::::::::es::::: "+component+":::"+ startTaskExecuters +"::" + endTaskExecuters );


                            ExecutorSpecificStats statsSpecific= stats.get_specific();

                            boolean isbolt =statsSpecific.is_set_bolt();

                             if  (isbolt) {



                                  //////////////////////////////////

                                 //TopologyInfo topologyInfo = client.getTopologyInfoWithOpts(topoId, getInfoOptions);

                                 Double capacity= getBoltCapacity(client,topologyID,component);
                                 //System.out.println("+++++Capacity::::::::::::::: "+ component+":::::" +capacity ) ;
                                 //////////////////////////////////////////



                                 String statsBolt =statsSpecific.get_bolt().get_execute_ms_avg().toString() ;
                                 //String statsBolt =statsSpecific.

                                 //String statsBoltnew =statsSpecific..get_common().get_json_conf());
                                 //.getValue().get_common().get_json_conf());


                                 Map<String, Map<GlobalStreamId, Double>> statsBoltNew = statsSpecific.get_bolt().get_execute_ms_avg();

                                 int c= statsBoltNew.size();
                                 //System.out.println(":::::::::::::::::statsBolt:::::::::::::::" + component +"::::::"+  statsBolt   );


                                 Double executeAverageLatency=0.0;
                                 for (     Entry<String, Map<GlobalStreamId, Double>> entry : statsBoltNew.entrySet()  ) {
                                     Map<GlobalStreamId, Double> InfoSection = entry.getValue();
                                     for ( Entry <GlobalStreamId, Double> entry2 : InfoSection.entrySet()) {
                                          executeAverageLatency=entry2.getValue();
                                     }
                                 }

                                // System.out.println(":::::::::::::::::executeAverageLatency::::::::::::: +" +component +"::::::" + executeAverageLatency   );

                                 //formatDouble =
                                // executeAverageLatency = form(executeAverageLatency);

                                 DecimalFormat df2 = new DecimalFormat("#.###");
                                 String formateed = df2.format(executeAverageLatency);
                                 Double output = Double.parseDouble(formateed);
                                 executeAverageLatency =output;

                                // System.out.println(":::::::::::::::::executeAverageLatency::::::::::::: +" +component +"::::::" + executeAverageLatency   );
                                 //System.out.println("::component::"+component +"::::capacity::::::::: +" +capacity +"::::::" + capacity+"::::Execute::::" + executeAverageLatency );


                                 /// for benchmark topology
                                 result.clear() ;
                                 result.put(executeAverageLatency,capacity);
                                 //System.out.println(":::::::::::::::Monitorscheduling::result::::::::::::: +" +component +"::::::" + result.toString()   );


                                 /// for execeutors of normal topology
                                 if ( capacity != null || executeAverageLatency!= null )
                                 {

                                    if (!topologyID.startsWith("BenchMarkTask") )
                                    {
                                        newresult.put(capacity, executeAverageLatency);
                                      //  System.out.println(":::::::Monitor::newresultBenchmark::: "+topologyID+"::"+ component+ "::"+executeAverageLatency+"::"+capacity);

                                      //  System.out.println(":::::::Monitor::resultMetricToComponent::: "+ component+"::"+host+"::"+port+"::"+startTaskExecuters+"-"+endTaskExecuters+ newresult);
                                        resultMetricToComponent.put (component+"::"+host+"::"+port+"::"+startTaskExecuters+"-"+endTaskExecuters, newresult);


                                    }else
                                    {

                                        newresultBenchmark.put(capacity, executeAverageLatency);
                                       // newresult.put(capacity, executeAverageLatency);
                                        //System.out.println(":::::::::Monitor::newresultBenchmark::: " + newresultBenchmark);
                                        //resultMetricToComponentBenchmark.put (component, newresultBenchmark);
                                        resultMetricToComponentBenchmark.put(capacity, executeAverageLatency);

                                       // System.out.println(":::::::::Monitor::resultMetricToComponentBenchmark::: " + resultMetricToComponentBenchmark);


                                    }




                                     //resultComponent.p
                                 }

                             }

                            Map<String, Long> e2 = statted.get(":all-time");

                            if (e2 != null) {
                                Long stream = e2.get(this.stream);
                                if (stream != null) {
                                    streamFound = true;
                                    totalStatted += stream;
                                }
                            }
                        }
                    }
                }
            }


        }

        //System.out.println("::::::::::Going to cll  106::::::stats.get_emitted::::: " );
        if (componentParallelism <= 0) {
            HashSet<String> components = getComponents(client, topologyID);
           // System.out.println("Available components for " + topologyID + " :");
           // System.out.println("------------------");
            for (String comp : components) {
                //System.out.println(comp);
            }
            System.out.println("------------------");
            throw new IllegalArgumentException("component: " + component + " not found");
        }

        if (!streamFound) {
            throw new IllegalArgumentException("stream: " + stream + " not found");
        }
        long timeDelta = now - state.getLastTime();
        long stattedDelta = totalStatted - state.getLastStatted();
        state.setLastTime(now);
        state.setLastStatted(totalStatted);
        double throughput = (stattedDelta == 0 || timeDelta == 0) ? 0.0 : ((double) stattedDelta / (double) timeDelta);

//        System.out.println(topologyID + "\t"
//                + component + "\t"
//                + componentParallelism + "\t"
//                + stream + "\t"
//                + timeDelta + "\t"
//                + stattedDelta + "\t"
//                + throughput);
    }




    public static Double  getBoltCapacity(Nimbus.Iface client, String topologyID, String component) throws TException {
        Double  result=0.0;
        String topoId=topologyID;
        boolean sys =false;
        //System.out.println("+++++++++topologyID::::: "+ topologyID );
        GetInfoOptions getInfoOptions = new GetInfoOptions();
        getInfoOptions.set_num_err_choice(NumErrorsChoice.ONE);
        TopologyInfo topologyInfo = client.getTopologyInfoWithOpts(topoId, getInfoOptions);
        StormTopology stormTopology = client.getTopology(topoId);
        Map<String, List<ExecutorSummary>> boltSummaries = getBoltExecutors(topologyInfo.get_executors(), stormTopology, sys);
        //   Map<String, List<ExecutorSummary>> spoutSummaries = getSpoutExecutors(topologyInfo.get_executors(), stormTopology);
         Map<String, Bolt> boltSpecs = stormTopology.get_bolts();
         DecimalFormat df2 = new DecimalFormat("#.###");

        //System.out.println("++++++++++boltSpecs::::: "+ boltSpecs.toString() );
        for (Map.Entry<String, Bolt> boltEntry : boltSpecs.entrySet()) {
            String boltComponentId = boltEntry.getKey();

            ///
            //Bolt blt=boltEntry.getValue();
            //Map<GlobalStreamId, Grouping> boltinput = blt.get_common().get_inputs();
            //System.out.println("++++++++++bolt   jsconf::::: "+ blt +"::::" +  boltinput.toString() );
            //System.out.println("++++++++++boltComponentId::::: "+ boltComponentId.toString() );
            if (boltSummaries.containsKey(boltComponentId) && (sys || !Utils.isSystemId(boltComponentId))) {

                if (!boltComponentId.startsWith("__acker")  && !boltComponentId.startsWith("log")) {
               // if (!boltComponentId.startsWith("__acker")  ) {

                    if ( boltComponentId.equals(component)) {
                        result = StatsUtil.computeBoltCapacity(boltSummaries.get(boltComponentId));
                        String formateed = df2.format(result);
                        //System.out.println("double : " + String.format("%.2f", input));
                        Double capacity = Double.parseDouble(formateed);
                        result = capacity;
                        //System.out.println(":::::::::Capacity::::: " + ":::::::" + boltComponentId + "::::" + capacity);
                        //System.out.println("::::::::Capacity::::: " + ":::boltComponentId::::" + boltComponentId + formateed)  ;
                    }

                }

            }


        }

        return result;
    }


        public static Map<String, List<ExecutorSummary>> getBoltExecutors(List<ExecutorSummary> executorSummaries,
                                                                      StormTopology stormTopology, boolean sys) {
        Map<String, List<ExecutorSummary>> result = new HashMap();
        for (ExecutorSummary executorSummary : executorSummaries) {
            if (StatsUtil.componentType(stormTopology, executorSummary.get_component_id()).equals("bolt")
                    && (sys || !Utils.isSystemId(executorSummary.get_component_id()))) {
                List<ExecutorSummary> executorSummaryList = result.getOrDefault(executorSummary.get_component_id(), new ArrayList());
                executorSummaryList.add(executorSummary);
                result.put(executorSummary.get_component_id(), executorSummaryList);
            }
        }
        return result;
    }


    public static Map <String , Map<Double, Double> >   getMetricToComponent(){

        Map <String , Map<Double, Double> > result =  new HashMap<>();

        result= resultMetricToComponent;
        //resultMetricToComponent.clear() ;
        return result;
    }


    public static void clearMetricToComponent() {
        resultMetricToComponent.clear() ;
    }



    public static  Map<Double, Double>   getMetricToComponentBenchMarkTask(){

       // System.out.println("::::::calling :::getMetricToComponentBenchMarkTask::::: " );
        Map<Double, Double>    result =  new HashMap<>();

        result= resultMetricToComponentBenchmark;
        //System.out.println(":::::::::getMetricToComponentBenchMarkTask::::: " + result);
        return result;
    }


    public static void clearMetricToComponentBenchMarkTask() {
        resultMetricToComponentBenchmark.clear() ;
    }










    public void setInterval(int interval) {
        this.interval = interval;
    }

    public void setTopology(String topology) {
        this.topology = topology;
    }

    public void setTopologyID(String topologyID) {
        this.topologyID = topologyID;
    }



    public void setComponent(String component) {
        this.component = component;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public void setWatch(String watch) {
        this.watch = watch;
    }

    private static class MetricsState {
        private long lastTime = 0;
        private long lastStatted = 0;

        private MetricsState(long lastTime, long lastStatted) {
            this.lastTime = lastTime;
            this.lastStatted = lastStatted;
        }

        public long getLastStatted() {
            return lastStatted;
        }

        public void setLastStatted(long lastStatted) {
            this.lastStatted = lastStatted;
        }

        public long getLastTime() {
            return lastTime;
        }

        public void setLastTime(long lastTime) {
            this.lastTime = lastTime;
        }
    }

    private static class Poller {
        private long startTime = 0;
        private long pollMs = 0;

        private Poller(long startTime, long pollMs) {
            this.startTime = startTime;
            this.pollMs = pollMs;
        }

        public long nextPoll() throws InterruptedException {
            long now = System.currentTimeMillis();
            long cycle = (now - startTime) / pollMs;
            long wakeupTime = startTime + (pollMs * (cycle + 1));
            long sleepTime = wakeupTime - now;
            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
            now = System.currentTimeMillis();
            return now;
        }


        private double formatDouble(double input) {
            DecimalFormat df2 = new DecimalFormat("#.###");
            String formateed = df2.format(input);
            Double output = Double.parseDouble(formateed);
            return output;

        }


        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getPollMs() {
            return pollMs;
        }

        public void setPollMs(long pollMs) {
            this.pollMs = pollMs;
        }
    }







}
