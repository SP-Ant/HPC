package org.apache.storm.scheduler;

import org.apache.storm.generated.WorkerResources;
import org.apache.storm.scheduler.resource.RAS_Node;
import org.apache.storm.scheduler.resource.RAS_Nodes;
import org.apache.storm.shade.com.google.common.collect.Sets;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class RSPAntColonyScheduler {

    //  %% ACO Parameters
    public static int MaxIt;    //  % Maximum Number of Iterations
    public static int nAnt;      //  % Number of Ants (Population Size)

    public static int antIdx;
    public static int itrIdx;

    public static int Q;
    public static double tau0;
    //  double  tau0=10*Q/(nVar*mean(model.D(:)));	% Initial Pheromone
    public static double alpha;        //% Phromone Exponential Weight
    public static double beta;       //  % Heuristic Exponential Weight
    public static double rho;     //  % Evaporation Rate
    public static double numberStates;
    public static String topologyID = "";
    public static boolean searchPhase = true;
    public static int roundnumber = 0;


    public static List<WorkerSlot> availableSlots = new ArrayList<WorkerSlot>();
    public static List<WorkerSlot> previousAssignedSlots = new ArrayList<WorkerSlot>();
    public static List<ExecutorDetails> executorToAssign = new ArrayList<ExecutorDetails>();
    public static List<ExecutorDetails> spoutAcker = new ArrayList<ExecutorDetails>();

    public static List<AntSaveTrack> AntSaveTrackList = new ArrayList<AntSaveTrack>();
    public static AntSaveTrack antsBestTrack = new AntSaveTrack();
    public static Double antsBestCost =0.0;
    public static Double previousTopologyExecuteLatency =0.0;
    public static Map < Integer  , AntSaveTrack > antsBestPathSavePerIteration = new HashMap<>();



    public static List<TauPheromoneLevelMatrix> tauPheromoneLevelMatrix = new ArrayList<TauPheromoneLevelMatrix>();
    public static List<EtaHeuristicInformation> etaHeuristicInformation = new ArrayList<EtaHeuristicInformation>();


    //public static Map <ExecutorDetails , Map<WorkerSlot, Double> > tauPheromoneLevelMatrix = new HashMap<>();
    // public static Map <ExecutorDetails , Map<WorkerSlot, Double> > etaHeuristicInformation = new HashMap<>();


    public static Map<ExecutorDetails, WorkerSlot> execToSlotNew = new HashMap<>();


    public static boolean FirstRun = false;
    public static boolean firstassignspout = false;


    boolean append = true;

    /////////////   binpacking   ///////////

    public static TopologyDetails glbtopologyDetailes= null;
    public static  boolean FirstRunScheduling = false;
    public static boolean isBenchMarkeTasksKilled = false;
    public static  Map <String , Map<Double, Double> > supervisorCapacity = new HashMap<>();
    public static  Map <String , ExecutorDetails> executersToSupervisors = new HashMap<>();
    public static  Map <String , Map<Double, Double> > componentsResources = new HashMap<>();
    public static  Map <ExecutorDetails, Map<Double, Double> > executersResources = new HashMap<>();
    public static String topologyName = "";
    public static String componentNameMigrated = "";
    public static Map<ExecutorDetails, WorkerSlot> execToSlotOriginalAssignment = new HashMap<>();
    public static Map<ExecutorDetails, WorkerSlot> execToSlotAfterRebalance = new HashMap<>();
    public static Set<ExecutorDetails> topologexecuters = null;
    public static  Map <String ,LinkedList <ExecutorDetails> > supervisorsExecutersLinkedList = new HashMap<>();
    public static Map<String, List<ExecutorDetails>>   compToExecuters;

    /////////////////////////////////////////////






    public static void getTopologiesInfo(Cluster cluster, Topologies topologies) {
        Map<String, Map<String, Map<Double, Double>>> topologyCompponentsInfo = new HashMap<>();
        // topologyCompponentsInfo = GetExecuterDetails.getExecuterDetailsDaemon();
        //  System.out.println(".. Ant Colony ComponentsInfo.." +topologyCompponentsInfo );
        //System.out.println(".. Ant Colony getTopologiesInfo.."  );
        Collection<WorkerSlot> used_ports = cluster.getUsedSlots();
        if (used_ports.size() > 0) schedule(cluster, topologies);
    }

    //// bin packing ///////



    public static  void  scheduleBinpacking(Topologies topologies, Cluster cluster) {

        // logger("..Online Scheduling Assignment "  + ".."+ exec +"..."+wslot);
        String topologyId="";
        Topologies topologyCount= cluster.getTopologies();

        //boolean a= true; if(a) return;

        //  System.out.println("..Bin Packing.schedule......topologyCount....  " + topologyCount + "..........." );
        if (!topologyCount.toString().contains("Mytopology")) return;
        //   System.out.println("..Bin Packing.schedule......after adding....  " + topologyCount + "..........." );




        if (!FirstRunScheduling ) {


            Map<String, SupervisorDetails> supervisordetls = cluster.getSupervisors();
            for (Entry<String, SupervisorDetails> entry : supervisordetls.entrySet()) {
                String id_supervisor = entry.getKey();
                SupervisorDetails supdetails = entry.getValue();
                String supervisorid = supdetails.getId();
                double totalcpu = supdetails.getTotalCpu();
                double totalmem = supdetails.getTotalMemory();
                Map<Double, Double> capacity = new HashMap<>();
                capacity.put(totalcpu, totalmem);
                supervisorCapacity.put(id_supervisor, capacity);


                LinkedList<ExecutorDetails> linkedlistExecuters = new LinkedList<>();    /// for detecting executers in each bin
                supervisorsExecutersLinkedList.put(id_supervisor, linkedlistExecuters);


                // System.out.println("..Bin Packing.Supervisor......totalcpu....  " + supervisorid + "..........." + totalcpu + "..." + totalmem);
            }
        }


        List<ExecutorDetails> orderedExecutors=null;
        for (TopologyDetails topology : cluster.getTopologies()) {




            topologyId = topology.getId().toString();
            topologyID =  topology.getId().toString();

            // System.out.println("..Bin Packing.schedule2......topologyID....  " + topologyID + "..........." );
            if (!topologyID.toString().contains("Mytopology")) continue;
            //  System.out.println("..Bin Packing.schedule3......topologyID....  " + topologyID + "..........." );
            // System.out.println("..Bin Packing.schedule..........  " + topologyID + "..........." );

            // " MytopologyTest"

            double cpuNeeded = topology.getTotalRequestedCpu();
            Map<String, Component> topologyComp =  topology.getComponents();
            Set<ExecutorDetails> topologexecuters =  topology.getExecutors();








            //System.out.println("..Bin Packing.schedule..3..  "+ topologyCount);
            String topoId = topologyId;
            SchedulerAssignment assignment = cluster.getAssignmentById(topoId);
            RAS_Nodes nodes;
            nodes = new RAS_Nodes(cluster);

            Map<String, String> superIdToRack = new HashMap<>();
            Map<String, String> superIdToHostname = new HashMap<>();
            Map<String, List<RAS_Node>> hostnameToNodes = new HashMap<>();


            for (RAS_Node node: nodes.getNodes()) {
                String superId = node.getId();
                //  System.out.println("...RAS......superId....  "  + superId  );
                String hostName = node.getHostname();
                //  System.out.println("...RAS......hostName....  "  + hostName  );
                //String rackId = hostToRack.getOrDefault(hostName, DNSToSwitchMapping.DEFAULT_RACK);
                superIdToHostname.put(superId, hostName);
                // superIdToRack.put(superId, rackId);
                hostnameToNodes.computeIfAbsent(hostName, (hn) -> new ArrayList<>()).add(node);
                //  rackIdToNodes.computeIfAbsent(rackId, (hn) -> new ArrayList<>()).add(node);
            }




            if (topologyID.toString().contains("Mytopology"))
            {
                // System.out.println("..Bin Packing.Supervisor......topologyID.1...  " + topologyID);
                getComponentPageInfo(  topologyID  ,cluster );

            }



            // boolean a= true; if(a) return;

            // System.out.println(".................schedule check Slots........  " );
            Collection <WorkerSlot>  usedSlots = cluster.getUsedSlots() ;
            if (usedSlots.size() == 0) return ;
            //System.out.println(".................schedule check Slots....size....  " + usedSlots.size());

            if (!FirstRunScheduling ) {


                //System.out.println(".................Inside real scheduling ........  " );
                // hamid  List<WorkerSlot>   avilabslots = cluster.getAssignableSlots() ;


                List<WorkerSlot>   avilabslots = cluster.getAvailableSlots();

                System.out.println(".................Inside real scheduling ........  " );
                // System.out.println(".................avilabslots ........  "+ avilabslots );
                for (WorkerSlot slot : avilabslots) {
                    cluster.freeSlot(slot);
                    // System.out.println(".................avilabslots ........  "+ avilabslots );

                }

                isBenchMarkeTasksKilled =true;


                // System.out.println(".................slotes and topology freed ........  " );




                Collection<ExecutorDetails> unassignedExecutors = new HashSet  <>  (cluster.getUnassignedExecutors(glbtopologyDetailes));
                if (unassignedExecutors.size() ==0) {
                    System.out.println("..Bin Packing.Supervisor......unassignedExecutors null....  ");
                    return;
                }
                orderedExecutors = orderExecutors(glbtopologyDetailes, unassignedExecutors);
                System.out.println("..Bin Packing.Supervisor......orderedExecutors....  " + orderedExecutors);
                //System.out.println(".....Executors....executersResources..  " + executersResources);





                Collection <TopologyDetails> topologydetails= cluster.getTopologies().getTopologies();
                for (TopologyDetails td : topologydetails) {
                    compToExecuters = cluster.getNeedsSchedulingComponentToExecutors( td );
                    // System.out.println("........Schedule....compToExecuters..."+ compToExecuters );
                }
                System.out.println("........Schedule....assign Executers..."+ compToExecuters );
                assignExecuters(topologies, cluster, orderedExecutors);

                FirstRunScheduling=true;
            }



        }  /// main for loop topologies

    }





    public static  void  assignExecuters(Topologies topologies, Cluster cluster , List<ExecutorDetails> orderedExecutors) {


        int cnt=0;
        execToSlotOriginalAssignment.clear() ;
        WorkerSlot lastWorkerSlotAcker=null;


        for (ExecutorDetails lstexecutersorderItem : orderedExecutors)
        {

            Double cpuExecuter = 0.0;
            Double memExecuter = 0.0;
            ExecutorDetails executerName = null;
            // execToSlotOriginalAssignment.clear() ;


            for (Entry <ExecutorDetails, Map<Double, Double> > entry : executersResources.entrySet())
            {
                // System.out.println("..assignExecuters..  executersResources..  " +  executersResources );
                // executerName = null;
                Map<Double, Double> resourcesNeeded = null;

                executerName = entry.getKey();
                resourcesNeeded = entry.getValue();

                //System.out.println("..executerName....  "  +  executerName );

                if (lstexecutersorderItem.toString().equals(executerName.toString()))
                {
                    // totalCpu = Double.parseDouble(obj.toString());


                    for (Entry <Double, Double> entry1 : resourcesNeeded.entrySet())
                    {
                        cpuExecuter =entry1.getKey();
                        memExecuter =entry1.getValue();
                    }




                }
            }



            for ( Entry <String , Map<Double, Double> > entry : supervisorCapacity.entrySet()) {
                String id_supervisor = "";
                Double cpuTotalSupervisor =0.0;
                Double memTotalSupervisor =0.0;
                Map<Double, Double> resources = null;

                id_supervisor = entry.getKey();
                resources = entry.getValue();

                for (Entry <Double, Double> entry1 : resources.entrySet())
                {
                    cpuTotalSupervisor =entry1.getKey();
                    memTotalSupervisor =entry1.getValue();
                }

                // System.out.println("..id_supervisor....  " +  id_supervisor +"..cpuTotalSupervisor.."+cpuTotalSupervisor + "..memTotalSupervisor.."+ memTotalSupervisor);

                if (  (cpuExecuter < cpuTotalSupervisor)  &&  (  memExecuter < memTotalSupervisor )   )
                {
                    cnt++;
                    String id_supervisorNew  =id_supervisor  + "-" +cnt;

                    executersToSupervisors.put( id_supervisorNew , lstexecutersorderItem );
                    System.out.println("...Bin packing Assignment..Executer..."  + lstexecutersorderItem +"...To.." + id_supervisorNew);
                    cpuTotalSupervisor -= cpuExecuter;
                    memTotalSupervisor -= memExecuter;




                    /// we update the resources of supervisors
                    resources.clear() ;
                    resources.put(cpuTotalSupervisor, memTotalSupervisor);

                    supervisorCapacity.replace( id_supervisor , resources ) ;


                    WorkerSlot wslot=  SupervisorToWslot( cluster,id_supervisor );
                    lastWorkerSlotAcker = wslot;
                    execToSlotOriginalAssignment.put( lstexecutersorderItem,  wslot );
                    // System.out.println("...Bin packing Assignment..execToSlotOriginalAssignment.."  + execToSlotOriginalAssignment );

                    /////// update bin-Excuters
                    updateSupervisorsExecutersLinkedList(  id_supervisor ,    lstexecutersorderItem );
                    //System.out.println("......................................................................" );

                    break;

                }

            }




        }



        // System.out.println("...Bin packing executersToSupervisors..."  + executersToSupervisors );

        //  System.out.println("...Bin packing supervisorsExecutersLinkedList..."  + supervisorsExecutersLinkedList );


        Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();
        /// inserting acker executers to assignmet

        ExecutorDetails ackerExecuter =null;

        for (ExecutorDetails topologyexecuters : topologexecuters)
        {
            if (topologyexecuters.toString().contains("1, 1") )
            {
                execToSlotOriginalAssignment.put( topologyexecuters,  lastWorkerSlotAcker );
                ackerExecuter = topologyexecuters;

                break;
            }
        }
        /////////////////


        /// we update the Reward Matrix for migration
        /// it consists of supervisors and their free capacity
        for ( Entry <String , Map<Double, Double> > entry : supervisorCapacity.entrySet()) {
            String id_supervisor = "";
            Double cpuTotalSupervisor = 0.0;
            Double memTotalSupervisor = 0.0;
            Map<Double, Double> resources = null;

            id_supervisor = entry.getKey();
            resources = entry.getValue();

            for (Entry <Double, Double> entry1 : resources.entrySet())
            {
                cpuTotalSupervisor =entry1.getKey();
                memTotalSupervisor =entry1.getValue();
            }
            // supervisorsRewardFreeCapacity.put ( id_supervisor , cpuTotalSupervisor );
        }





        SchedulerAssignment newAssignment = new SchedulerAssignmentImpl(topologyID, execToSlotOriginalAssignment, slotToResources, null);
        cluster.assign(newAssignment, true);
        System.out.println("....................execToSlotOriginalAssignment..............."  + execToSlotOriginalAssignment);

        execToSlotAfterRebalance.putAll( execToSlotOriginalAssignment );
        System.out.println(".. Bin Packing. assignment is  done. " );
        // logger("..New Bin Packing. assignment is  done... " );

    }








    public static void updateSupervisorsExecutersLinkedList( String id_supervisor ,  ExecutorDetails lstexecutersorderItem ) {


        for (Entry  <String ,LinkedList<ExecutorDetails> >  supervisorexecuter : supervisorsExecutersLinkedList.entrySet())
        {
            String  idSupervisor = supervisorexecuter.getKey();
            LinkedList<ExecutorDetails> linkedlistExecuters  = supervisorexecuter.getValue();

            if (id_supervisor.equals( idSupervisor))
            {
                linkedlistExecuters.add( lstexecutersorderItem ) ;
                supervisorsExecutersLinkedList.put( idSupervisor , linkedlistExecuters );
                // System.out.println("......after  update.............supervisorsExecutersLinkedList.put...................." +supervisorsExecutersLinkedList);
                break;
            }
        }



    }



    public static WorkerSlot SupervisorToWslot(Cluster cluster , String id_supervisor)
    {
        WorkerSlot result=null;


        //WorkerSlot avilabslots = null;
        // List<WorkerSlot>   avilabslots = cluster.getAssignableSlots();
        List<WorkerSlot>   avilabslots = cluster.getAvailableSlots();
        // System.out.println("..SupervisorToWslot. id_supervisor "+ id_supervisor  );
        // System.out.println("..SupervisorToWslot. avilabslots "+ avilabslots  );

        for (WorkerSlot slot : avilabslots) {
            // System.out.println("..Ant Colony.scedule. workerslot "+ port  );

            if (slot.toString().contains(id_supervisor) )
            {
                result = slot;
                // System.out.println("........Return ..slot.... "+ result );
                break;
            }

        }
        return result;

    }




    public static List<ExecutorDetails> orderExecutors(
            TopologyDetails td, Collection<ExecutorDetails> unassignedExecutors) {
        Map<String, Component> componentMap = td.getComponents();

        //td.getTopology().

        List<ExecutorDetails> execsScheduled = new LinkedList<>();

        Map<String, Queue<ExecutorDetails>> compToExecsToSchedule = new HashMap<>();
        for (Component component : componentMap.values()) {

            //component.getExecs().

            //System.out.println("...Bin Packing......orderExecutors..1.orderExecutors.  "  + component.toString()  );
            compToExecsToSchedule.put(component.getId(), new LinkedList<ExecutorDetails>());
            for (ExecutorDetails exec : component.getExecs()) {
                if (unassignedExecutors.contains(exec)) {
                    compToExecsToSchedule.get(component.getId()).add(exec);
                    //System.out.println("...Bin Packing...orderExecutors.2..compToExecsToSchedule...  "  + compToExecsToSchedule.toString()  );

                }
            }
        }

        // System.out.println("..........................1..............................  "   );

        Set<Component> sortedComponents = sortComponents(componentMap);


        //System.out.println("..........................2..............................  "   );
        //System.out.println("...Bin Packing...sortedComponents.1..full...  "  + sortedComponents.toString()  );
        // System.out.println("..........................3..............................  "   );
        sortedComponents.addAll(componentMap.values());
        // System.out.println("...Bin Packing...sortedComponents..2.full...  "  + sortedComponents.toString()  );

        for (Component currComp : sortedComponents) {
            Map<String, Component> neighbors = new HashMap<String, Component>();
            for (String compId : Sets.union(currComp.getChildren(), currComp.getParents())) {
                neighbors.put(compId, componentMap.get(compId));
                // System.out.println("...Bin Packing...neighbors...  "  + neighbors  );
            }
            Set<Component> sortedNeighbors = sortNeighbors(currComp, neighbors);
            // System.out.println("...Bin Packing...sortNeighbors..full.  "  + sortedNeighbors  );
            Queue<ExecutorDetails> currCompExesToSched = compToExecsToSchedule.get(currComp.getId());

            boolean flag = false;
            do {
                flag = false;
                if (!currCompExesToSched.isEmpty()) {
                    execsScheduled.add(currCompExesToSched.poll());
                    // System.out.println("...Bin Packing...execsScheduled.  "  + execsScheduled  );
                    //System.out.println("...Bin Packing...execsScheduled.  "  + currCompExesToSched  );
                    flag = true;
                }

                for (Component neighborComp : sortedNeighbors) {
                    Queue<ExecutorDetails> neighborCompExesToSched = compToExecsToSchedule.get(neighborComp.getId());
                    if (!neighborCompExesToSched.isEmpty()) {
                        execsScheduled.add(neighborCompExesToSched.poll());
                        flag = true;
                    }
                }
            } while (flag);
        }
        return execsScheduled;
    }




    public static  Set<Component> sortComponents(final Map<String, Component> componentMap) {
        Set<Component> sortedComponents =
                new TreeSet<>((o1, o2) -> {
                    int connections1 = 0;
                    int connections2 = 0;

                    for (String childId : Sets.union(o1.getChildren(), o1.getParents())) {
                        //  System.out.println("...Bin Packing...sortComponents..1. o1.getChildren() "  + o1.getChildren()  );
                        //  System.out.println("...Bin Packing...sortComponents...1 o1.getParents() "  + o1.getParents()  );
                        connections1 += (componentMap.get(childId).getExecs().size() * o1.getExecs().size());

                        //  System.out.println("...Bin Packing...sortComponents..1. componentMap get(childId)"  + componentMap.get(childId)  );
                        //  System.out.println("...Bin Packing...sortComponents..1. componentMap.get(childId).getExecs().size()"  + componentMap.get(childId).getExecs().size()  );
                        //  System.out.println("...Bin Packing...sortComponents..1. o1.getExecs().size()"  + o1.getExecs().size()  );

                        // System.out.println("...Bin Packing...sortComponents..1. connections1 "  + connections1  );
                    }

                    for (String childId : Sets.union(o2.getChildren(), o2.getParents())) {

                        //System.out.println("...Bin Packing...sortComponents..2. o1.getChildren() "  + o2.getChildren()  );
                        //System.out.println("...Bin Packing...sortComponents..2 o1.getParents() "  + o2.getParents()  );

                        connections2 += (componentMap.get(childId).getExecs().size() * o2.getExecs().size());

                        //System.out.println("...Bin Packing...sortComponents..2. componentMap get(childId)"  + componentMap.get(childId)  );
                        //System.out.println("...Bin Packing...sortComponents..2. componentMap.get(childId).getExecs().size()"  + componentMap.get(childId).getExecs().size()  );
                        //System.out.println("...Bin Packing...sortComponents..2. o1.getExecs().size()"  + o2.getExecs().size()  );

                        //System.out.println("...Bin Packing...sortComponents..2. connections1 "  + connections1  );

                    }

                    if (connections1 > connections2) {
                        return -1;
                    } else if (connections1 < connections2) {
                        return 1;
                    } else {
                        return o1.getId().compareTo(o2.getId());
                    }
                });
        sortedComponents.addAll(componentMap.values());
        return sortedComponents;
    }


    public static Set<Component> sortNeighbors(
            final Component thisComp, final Map<String, Component> componentMap) {
        Set<Component> sortedComponents =
                new TreeSet<>((o1, o2) -> {
                    int connections1 = o1.getExecs().size() * thisComp.getExecs().size();
                    int connections2 = o2.getExecs().size() * thisComp.getExecs().size();
                    if (connections1 < connections2) {
                        return -1;
                    } else if (connections1 > connections2) {
                        return 1;
                    } else {
                        return o1.getId().compareTo(o2.getId());
                    }
                });
        sortedComponents.addAll(componentMap.values());
        return sortedComponents;
    }








    public static  void getComponentPageInfo(String  topologyID , Cluster cluster ) {

        // System.out.println(".....getTopologyCompleteTime::::::topologyID::::" + topologyID);
        //  Map<String, Map<String, Double>> topologyycompleteTime =  MonitorScheduling.getTopologyExecuteCompleteTime( topologyID);

        String component = "";
        Map<String, Component> topologyComp = null;
        //topologexecuters.clear() ;
        //Set<ExecutorDetails> topologexecuters = null;
        // Collection<TopologyDetails>  topologyDetail = topologies.getTopologies();

        for (TopologyDetails topology : cluster.getTopologies()) {

            String topologyId = topology.getId().toString();
            if (topologyId.toString().equals( topologyID )) {
                double cpuNeeded = topology.getTotalRequestedCpu();
                topologyComp = topology.getComponents();
                topologexecuters = topology.getExecutors();
                break;
            }else
            {
                continue;
            }
            // System.out.println(".......topologexecuters...." + topologexecuters);

        }

        String strComp = "";
        Component comp = null;

        for (Entry<String, Component> entry : topologyComp.entrySet()) {

            strComp = entry.getKey();
            comp = entry.getValue();
            List<ExecutorDetails> lstExecs=  comp.getExecs();
            // System.out.println(".......str....." + strComp + ".......comp....." + comp);

            Map<String, Object> resultGetComponentPage = new HashMap<>();
            resultGetComponentPage = GetExecuterDetails.getComponentPage(topologyID, strComp);
            //   resultGetComponentPage = GetExecuterDetails.getComponentPage(topologyID, "counter");

            //  System.out.println(".....Bin Packing.  getComponentPageInfo..  "+ resultGetComponentPage);

            Double totalCpu = 0.0;
            Double totalMem = 0.0;
            for (Entry<String, Object> entry1 : resultGetComponentPage.entrySet()) {
                String str1 = "";
                Object obj = null;
                str1 = entry1.getKey();
                obj = entry1.getValue();
                // System.out.println(".........str....  " +str1  +"......obj......." + obj);
                if (str1.equals("requestedCpu"))  totalCpu = Double.parseDouble(obj.toString());
                if (str1.equals("requestedMemOnHeap")) totalMem = Double.parseDouble(obj.toString());

                // System.out.println(".........obj..  " +obj  );
                //  System.out.println("................................................................................  "   );
            }
            // System.out.println("...strComp..  " + strComp+"...requestedCpu..  " + totalCpu + "....requestedMemOnHeap..  " + totalMem);
            // System.out.println(".........requestedMemOnHeap..  " + totalMem);

            int numberofExecutersPerComponent=lstExecs.size();

            for (ExecutorDetails lstexecsAssignResources : lstExecs) {
                // System.out.println(".....Executors....lstexecsAssignResources..  " + lstexecsAssignResources);
                Map<Double, Double> capacityExecuters = new HashMap<>();
                capacityExecuters.put(totalCpu/numberofExecutersPerComponent, totalMem/numberofExecutersPerComponent);
                executersResources.put (lstexecsAssignResources,capacityExecuters );

            }

            //  System.out.println("..getComponentPageInfo      .executersResources..  " +   executersResources );

            Map<Double, Double> capacity = new HashMap<>();
            capacity.put(totalCpu, totalMem);
            componentsResources.put (strComp,capacity );











        }



    }



    public static void logger(String info, String fileName,Boolean append)
    {


        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        System.out.println(dateFormat.format(cal.getTime()));
        String fileLoggerName = fileName;


        // logger("..Online Scheduling Assignment " +  dateFormat.format(cal.getTime()));
        FileWriter fw = null;
        BufferedWriter bw = null;
        PrintWriter pw = null;
        try {
            // fw = new FileWriter("/home/ali/work/apache-storm-2.1.0/logs/OnlineScheduling.log", true);
            if (!append ) {
                fw = new FileWriter("/home/ali/work/apache-storm-2.1.0/logs/" + fileLoggerName + ".log", false);
            }else
            {
                fw = new FileWriter("/home/ali/work/apache-storm-2.1.0/logs/" + fileLoggerName + ".log", true);
            }

            bw = new BufferedWriter(fw);
            pw = new PrintWriter(bw);

            pw.println( dateFormat.format(cal.getTime())+"," + info+"/r/n");

            pw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static class ActionEvaluation
    {

        List<ExecutorDetails>   executerDetails =null;
        String  sourceSupervisorID =null;
        String  targetSupervisorID =null;
        Double costValueBeforMigration =0.0;
        Double costValueAfterMigration =0.0;
        Map<ExecutorDetails, WorkerSlot> execToSlotAfterRebalanceEvaluation =new HashMap<>();
        int epochnumber;
        Long inputRate = 0L;


    }





    //////////////////////////////////////////////////////






    public static void schedule(Cluster cluster, Topologies topologies) {

        ////  bin-packing algorithm is implemented in htis class.
        ///  firstly we get assignments from bin-packing

        boolean initialDone = initialEnvironment(cluster, topologies);
        //System.out.println(".. Ant Colony schedule.." +initialDone );
//        int a = 10;
//        if (a > 0) return;
        if (!initialDone) return;
        //  System.out.println("..Ant Colony.scedule  000 ");
        // if (FirstRun) return;   /// for controlling only run one time for test
        //int maxLimitRun =1;
        int maxLimitRun =150;


        if (roundnumber >= maxLimitRun) return;

        if (searchPhase) {
            if ( itrIdx < MaxIt )
            {
                if ( antIdx < nAnt )
                {
                    System.out.println("..Ant Colony.scedule         itrIdx .. " + itrIdx  +  ".. antIdx  .. " + antIdx) ;
                    System.out.println("................................................................................") ;
                    System.out.println("................................................................................") ;

                    logger("..Ant Colony.scedule         itrIdx .. " + itrIdx  +  ".. antIdx  .. " + antIdx); ;
                    logger("................................................................................") ;
                    logger("................................................................................") ;



                    boolean resultAssignment = assignExecutersByAnt(cluster, topologies);
                    antIdx++;
                    // FirstRun = true;
                    searchPhase = false;
                    return;
                }else  /// when all the ants one  perform iteration
                {
                    /// we save the best cost for ants per iteration
                    antsBestPathSavePerIteration.put( itrIdx , antsBestTrack);
                    antsBestCost =0.0;

                    antIdx =0;
                    itrIdx++;

                    System.out.println("..Ant Colony.scedule         itrIdx .. " + itrIdx  +  ".. antIdx  .. " + antIdx) ;
                    logger("..Ant Colony.scedule         itrIdx .. " + itrIdx  +  ".. antIdx  .. " + antIdx) ;
                    boolean resultAssignment = assignExecutersByAnt(cluster, topologies);
                    searchPhase = false;
                    return;

                }

            }
        } else    /// when monitoring and evaluating ants movements
        {
            boolean result = EvaluateandSaveAntMovement();

            if (result) {


                Double topologyExecuteLatencyms = GetExecuterDetails.getTopologyExecuteCompleteTime(topologyID);
                Double topologyExecuteLatency = topologyExecuteLatencyms;
                topologyExecuteLatency = formateDoubleValue(topologyExecuteLatency);

                if (topologyExecuteLatency == null || topologyExecuteLatency == 0.0){  /// waiting for  New value ....topologyExecuteLatency
                    System.out.println("....Scheduling. waiting for  New value ....topologyExecuteLatency.... " +  topologyExecuteLatency);
                    searchPhase = false;
                    return;
                }else{

                    searchPhase = true;  /// going to new assignment
                    System.out.println("...Scheduling ... going to new assignment..... " + result);

                    // FirstRun = true;
                    roundnumber++;
                }
            }
        }


    }


    public static boolean initialEnvironment(Cluster cluster, Topologies topologies) {

        //  %% ACO Parameters
        MaxIt = 5;
        //MaxIt = 1;
        // nAnt = 40;
        nAnt = 6;
        //nAnt = 1;
        Q = 1;
        tau0 = 0.0;
        //  double  tau0=10*Q/(nVar*mean(model.D(:)));	% Initial Pheromone
        alpha = 1.0;        //% Phromone Exponential Weight
        beta = 1.0;       //  % Heuristic Exponential Weight
        rho = 0.05;     //  % Evaporation Rate
        numberStates = 0.0;
        boolean initialDone = false;
        WorkerSlot avilablePort = null;
        availableSlots = cluster.getAssignableSlots();
        numberStates = availableSlots.size();
        for (WorkerSlot port : availableSlots) {
            // System.out.println("..Ant Colony.scedule. workerslot "+ port  );
        }


        for (TopologyDetails topology : cluster.getTopologies()) {
            topologyID = topology.getId().toString();
            //System.out.println("..Ant Colony.scedule. topologyID "+ topologyID  );
        }

        SchedulerAssignment assignment = cluster.getAssignmentById(topologyID);
        Map<ExecutorDetails, WorkerSlot> ex2ws = assignment.getExecutorToSlot();

        List<String> executorsSpoutAcker = GetExecuterDetails.getSpoutAckerExecuters(topologyID);
        executorToAssign.clear();
        spoutAcker.clear();
        for (ExecutorDetails executor : ex2ws.keySet()) {
            String executorId = executor.toString();
            boolean found = false;
            for (String spoutAckerExecuters : executorsSpoutAcker) {
                if (executorId.equals(spoutAckerExecuters.toString())) {
                    found = true;
                }
            }

            if (!found) {
                executorToAssign.add(executor);
            } else {
                spoutAcker.add(executor);
            }
        }

        /// initial tauPheromoneLevelMatrix Matrix and  etaHeuristicInformation

        if (tauPheromoneLevelMatrix.size() == 0) {  /// only for the first time
            for (ExecutorDetails executer : executorToAssign) {
                for (WorkerSlot wslot : availableSlots) {
                    TauPheromoneLevelMatrix listTauPheromoneLevel = new TauPheromoneLevelMatrix();
                    EtaHeuristicInformation listetaHeuristicInformation = new EtaHeuristicInformation();

                    listTauPheromoneLevel.executerDetails = executer;
                    listTauPheromoneLevel.workerSlot = wslot;
                    //listTauPheromoneLevel.value = 1.0;   /// it should be one at start
                    //listTauPheromoneLevel.value = 0.0;
                    listTauPheromoneLevel.value = 0.5;

                    listetaHeuristicInformation.executerDetails = executer;
                    listetaHeuristicInformation.workerSlot = wslot;
                    listetaHeuristicInformation.value = 1.0;  // we set the distance to 1 at the start


                    tauPheromoneLevelMatrix.add(listTauPheromoneLevel);
                    etaHeuristicInformation.add(listetaHeuristicInformation);
                }
            }

        }

        if (executorToAssign.size() > 0) {

            initialDone = true;

        }


        return initialDone;
    }

    public static void updateHeuristicAndPheromone(AntSaveTrack antsavetrack) {
        Map<ExecutorDetails, WorkerSlot> execToSlotNewassignment = new HashMap<>();
        Map<WorkerSlot, Double> updatedValue = new HashMap<>();

        //  Map <ExecutorDetails, Map<WorkerSlot, Double>> tempetaHeuristicInformation = new HashMap<>();

        execToSlotNew = antsavetrack.execToSlotNew;
        Double topologyExecuteLatency = antsavetrack.topologyExecuteLatency;
        //System.out.println("..............topologyExecuteLatency.................." + topologyExecuteLatency);


        for (EtaHeuristicInformation eta : etaHeuristicInformation) {   /// updating eta or visibility
            //System.out.println("..............etaHeuristicInformation.................." +eta.executerDetails+".."+".." +eta.workerSlot+".."+eta.value);
            ExecutorDetails execdetailsMatrix = eta.executerDetails;
            WorkerSlot wslotList = execToSlotNew.get(execdetailsMatrix);
            if (eta.workerSlot.toString().equals(wslotList.toString())) {
                Double value = Q / topologyExecuteLatency;
                eta.value = formateDoubleValue(value);
            }
        }


        for (TauPheromoneLevelMatrix tah : tauPheromoneLevelMatrix) {   /// updating pheromone by each ant
            ExecutorDetails execdetailsMatrix = tah.executerDetails;
            WorkerSlot wslotList = execToSlotNew.get(execdetailsMatrix);
            if (tah.workerSlot.toString().equals(wslotList.toString())) {

                Double value = tah.value + Q / topologyExecuteLatency;
                tah.value = formateDoubleValue(value);  /// updating pheromone level
            }
        }

    }


    public static WorkerSlot ProbabilitySelectWorkerNode( ExecutorDetails execdetails )    /// main funnction to select a worker node by Ant .
    {
        WorkerSlot SelectedNodePort =null;
        ExecutorDetails selectedExecuter = execdetails;
        Map< WorkerSlot , Double > tahValue = new HashMap<>();
        Map< WorkerSlot , Double > etaValue = new HashMap<>();
        Map< WorkerSlot , Double > tahEtah = new HashMap<>();
        List<Double> probapility = new ArrayList<Double>();
        List<WorkerSlot> selectedWorkerSlots = new ArrayList<WorkerSlot>();

        for (TauPheromoneLevelMatrix tahPehromoneMatrix : tauPheromoneLevelMatrix) {   /// updating pheromone by each ant

            if ( tahPehromoneMatrix.executerDetails.toString().equals(execdetails.toString() ))
            {
                tahValue.put( tahPehromoneMatrix.workerSlot , tahPehromoneMatrix.value);
            }
        }


        /// display tah
//        for (Entry< WorkerSlot, Double   > tah : tahValue.entrySet())
//        {
//            WorkerSlot wslot =    tah.getKey();
//            Double  valuetah =tah.getValue();
//            System.out.println(".......ProbabilitySelectWorkerNode.....tahValue...wslot." + wslot +".."+ valuetah );
//        }



        //       System.out.println(".................................................................");

        for (EtaHeuristicInformation etaHeuristicMatrix : etaHeuristicInformation) {
            if ( etaHeuristicMatrix.executerDetails.toString().equals(selectedExecuter.toString() ))
            {
                etaValue.put( etaHeuristicMatrix.workerSlot , etaHeuristicMatrix.value);
            }
        }

//        for (Entry< WorkerSlot, Double   > eta : etaValue.entrySet())
//        {
//            WorkerSlot wslot =    eta.getKey();
//            Double  valueeta =eta.getValue();
//            System.out.println(".......ProbabilitySelectWorkerNode.....eta...wslot." + wslot +".."+ valueeta );
//        }

        //       System.out.println(".................................................................");

        Double sum = 0.0;
        for (Entry< WorkerSlot, Double   > tah : tahValue.entrySet())  /// for display
        {
            WorkerSlot wslot =    tah.getKey();
            Double  valuetah =tah.getValue();
            Double  valueeta = etaValue.get(wslot);
            Double valueTahEta  = (  Math.pow(valuetah,alpha))  * (Math.pow(valueeta,beta)) ;
            valueTahEta = formateDoubleValue(valueTahEta);
            sum = sum + valueTahEta ;
            tahEtah.put ( wslot ,valueTahEta );
        }

        //       System.out.println(".......ProbabilitySelectWorkerNode.....tahmultiplyEtah...sum." + sum  );
        for (Entry< WorkerSlot, Double> tahmultiplyEtah : tahEtah.entrySet())   /// display
        {
            WorkerSlot wslot =    tahmultiplyEtah.getKey();
            Double  valuetaheta =tahmultiplyEtah.getValue();
            Double probabilityWorkerNode =  valuetaheta / sum;
            probabilityWorkerNode = formateDoubleValue(probabilityWorkerNode);
            probapility.add(probabilityWorkerNode) ; /// prepare the probabilities for the roulettwheelselection
            selectedWorkerSlots.add(wslot);
            //   System.out.println(".......ProbabilitySelectWorkerNode.....tahmultiplyEtah...wslot." + wslot +".."+ valuetaheta + "..." + probabilityWorkerNode.toString() );
        }

        int selectedIdx = rouletteWheelSelection(probapility);
        //  System.out.println(".....ProbabilitySelectWorkerNode..selectedIdx.....by rolletwheel...sum." + selectedIdx  + ".." + selectedWorkerSlots.get( selectedIdx ));
        SelectedNodePort =  selectedWorkerSlots.get( selectedIdx );
        return SelectedNodePort;


    }




    public static void  displayMatrixInformation()
    {

        for (EtaHeuristicInformation eta : etaHeuristicInformation) {

            DecimalFormat df2 = new DecimalFormat("#.####");
            String formateed = df2.format(eta.value);
            // System.out.println("........................etaHeuristicInformation................................" +eta.executerDetails+".."+".." +eta.workerSlot+".."+formateed);
            // logger("........................etaHeuristicInformation................................" +eta.executerDetails+".."+".." +eta.workerSlot+".."+formateed);
        }
        //  System.out.println(".........................................................................................");
        logger(".........................................................................................");

        for (TauPheromoneLevelMatrix tah : tauPheromoneLevelMatrix) {
            DecimalFormat df2 = new DecimalFormat("#.####");
            String formateed = df2.format(tah.value);

            //  System.out.println("........................tauPheromoneLevelMatrix......................................" +tah.executerDetails+".."+".." +tah.workerSlot+".."+formateed);
            //  logger("........................tauPheromoneLevelMatrix......................................" +tah.executerDetails+".."+".." +tah.workerSlot+".."+formateed);
        }
    }



    public static void  evaporatePheromone()
    {

        for (TauPheromoneLevelMatrix tah : tauPheromoneLevelMatrix) {   /// updating pheromone by each ant
            ExecutorDetails execdetailsMatrix = tah.executerDetails;
            WorkerSlot wslotList = execToSlotNew.get(execdetailsMatrix);
            if ( tah.workerSlot.toString().equals(wslotList.toString() )){
                Double value =( 1- rho) * tah.value ;
                tah.value =  formateDoubleValue(value);  /// updating pheromone level
                // System.out.println("........................etaHeuristicInformation....updated.................................." +eta.executerDetails+".."+".." +eta.workerSlot+".."+eta.value);
            }
        }


    }



    public static boolean  EvaluateandSaveAntMovement()
    {
        boolean result=false;

        if (searchPhase)
        {
            result=false;
            return result;
        }

        Double topologyExecuteLatencyms = GetExecuterDetails.getTopologyExecuteCompleteTime(topologyID);
        Double topologyExecuteLatency = topologyExecuteLatencyms  / 1000 ;  /// 1000
        topologyExecuteLatency =  formateDoubleValue(topologyExecuteLatency);
        System.out.println("... EvaluateandSaveAntMovement  topologyExecuteLatency "+ topologyExecuteLatencyms  + "..." + topologyExecuteLatency );

        logger("... EvaluateandSaveAntMovement  topologyExecuteLatency "+ topologyExecuteLatencyms  + "..." + topologyExecuteLatency );
        logger(".....Evalaute. antsBestCost ... "+  antsBestCost + "..."  + itrIdx +"..." +  antIdx   );
        if (topologyExecuteLatency == null || topologyExecuteLatency == 0.0  )
        {

            result=false;
            searchPhase = false;
            // System.out.println("... EvaluateandSaveAntMovement .result . " + result) ;
            //return result;
        }else {


            AntSaveTrack antsavetrack = new AntSaveTrack();
            antsavetrack.iterationID = antIdx;
            antsavetrack.antID = antIdx;
            antsavetrack.execToSlotNew = execToSlotNew;
            antsavetrack.topologyExecuteLatency = topologyExecuteLatency;
            AntSaveTrackList.add(antsavetrack);
            //previousTopologyExecuteLatency = topologyExecuteLatency;
            System.out.println("........Evalaute..............  .Save.....class Ant ... " + topologyExecuteLatency + "...");
            result =true;
            if ( antsBestCost == 0)   antsBestCost =  topologyExecuteLatency ;  /// first ant of each iteration
            if (    topologyExecuteLatency <    antsBestCost ) {
                antsBestCost = topologyExecuteLatency;
                antsBestTrack =  antsavetrack;
                System.out.println(".....Evalaute. antsBestCost ... "+  antsBestCost + "..."  + itrIdx +"..." +  antIdx   );
                logger(".....Evalaute. antsBestCost ... "+  antsBestCost + "..."  + itrIdx +"..." +  antIdx   );

            }

            updateHeuristicAndPheromone(   antsavetrack  );
            displayMatrixInformation();
            evaporatePheromone();

        }

        for (AntSaveTrack antstrackevaluate : AntSaveTrackList) {   /// display the ant movement

            // logger("..Ant Colony.scedule. list antspecmovement ... "+ antstrackevaluate.iterationID  );
            // logger("..Ant Colony.scedule. list antID ... "+ antstrackevaluate.antID  );
            // logger("..Ant Colony.scedule. list execToSlotNew ... "+ antstrackevaluate.execToSlotNew  );
            //  logger("..Ant Colony.scedule. list topologyExecuteLatency ... "+ antstrackevaluate.topologyExecuteLatency  );
            //   logger(".............................................................."  );

        }

        /// we save the best ant path for the iteration and also we save the ant track class for all information

        // displayMatrixInformation();

        //searchPhase = true;
        return result;
    }



    public static boolean assignExecutersByAnt(Cluster cluster, Topologies topologies)

    {
        boolean done=false;
        Random rand = new Random();
        Map<ExecutorDetails, WorkerSlot> executorToSlot = null;
        //Map<ExecutorDetails, WorkerSlot> execToSlotNew = new HashMap<>();
        execToSlotNew.clear();
        WorkerSlot nodePort = null;
        Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyID);
        if (existingAssignment != null) {
            executorToSlot = existingAssignment.getExecutorToSlot();
        }


        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : executorToSlot.entrySet())
        {
            ExecutorDetails execdetails =  entry.getKey();
            boolean found =false;
            for (ExecutorDetails  executorspout : spoutAcker)
            {    /// check if the executer is spout , bolt or acker
                String  spoutAckerexecuter = executorspout.toString() ;
                if ( execdetails.toString().equals(spoutAckerexecuter)  )
                {
                    found = true;
                    //System.out.println("..Ant Colony.scedule found " +execdetails.toString());
                }
            }
            if ( found )
            {
                nodePort = availableSlots.get(0);     /// spouts and ackers are assigned to first available slot
            }
            else
            {

                int numberofWorkernodes= availableSlots.size();
                int n=0;
                if (itrIdx == 0 && antIdx == 0 ){    /// only for the first time we assign the slots randomly
                    n = rand.nextInt(numberofWorkernodes);
                    nodePort = availableSlots.get(n);
                }else
                {
                    //  System.out.println("..Ant Colony.scedule ....assignExecutersByAnt..assign by calculating pheromone.");
                    WorkerSlot SelectedNodePort = ProbabilitySelectWorkerNode( execdetails );
                    nodePort = SelectedNodePort;
                }

            }
            execToSlotNew.put(execdetails,nodePort);
        }


        System.out.println("..Ant Colony.scedule available Slots "  + availableSlots.size());
        logger("..Ant Colony.scedule available Slots "  + availableSlots.size());
        for (Entry<ExecutorDetails, WorkerSlot> displayassignment : execToSlotNew.entrySet())  /// for display
        {
            ExecutorDetails exec =    displayassignment.getKey();
            WorkerSlot  wslot =displayassignment.getValue();
            System.out.println("..Ant Colony.scedule Random assignment "  + ".."+ exec +"..."+wslot);
            logger("..Ant Colony.scedule Random assignment "  + ".."+ exec +"..."+wslot);
        }

        cluster.unassign(topologyID);
        for (WorkerSlot  slot : cluster.getUsedSlots() ) {
            cluster.freeSlot(slot);
        }




        SchedulerAssignment newAssignment = new SchedulerAssignmentImpl(topologyID, execToSlotNew, slotToResources, null);
        cluster.assign(newAssignment, true);
        System.out.println("..new assignment is  done. " );
        logger("..new assignment is  done. " );
        done =true;
        return done;
    }




    public static class TauPheromoneLevelMatrix
    {
        ExecutorDetails   executerDetails =null;
        WorkerSlot workerSlot =null;
        Double value =0.0;
    }



    public static class EtaHeuristicInformation
    {
        ExecutorDetails   executerDetails =null;
        WorkerSlot workerSlot =null;
        Double value =0.0;
    }



    public static class AntSaveTrack
    {
        public int antID =0;
        public int iterationID =0;
        public Double topologyExecuteLatency =0.0;
        Map<ExecutorDetails, WorkerSlot> execToSlotNew = null;
        Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();

    }



    public static int rouletteWheelSelection(   List<Double> probabilities  )
    {

        int selectedIdx = 0;


        //List<Double> probabilities = new ArrayList<Double>();
        List<Double> cumSum = new ArrayList<Double>();
//        probabilities.add(0.1);
//        probabilities.add(0.2);
//        probabilities.add(0.3);
//        probabilities.add(0.4);

        //cumsum =  0.1  0.3  0.6  1.0

        Double sum = 0.0;
        for (int i = 0; i < probabilities.size(); i++) {
            // System.out.print(probabilities.get(i) + " ");
            if (i == 0) {
                cumSum.add(probabilities.get(i));
            } else {

                for (int j = 0; j < i; j++) {

                    sum = sum + probabilities.get(j);
                }
                // System.out.println( "sum ...." + sum   );
                Double totalvalues = sum + probabilities.get(i);
                sum =0.0;
                cumSum.add(totalvalues);
            }
        }


        double x = Math.random();
        //  System.out.println( "Random between 0 and 1 ...."+ x );
        // x= 0.42;
        int selectedidx =0;
        System.out.println( ".............................................................. ....");
        for (int i = 0; i < cumSum.size(); i++){
            //  System.out.println( "probabilities ...."+ cumSum.get(i) + " ");

        }
        System.out.println( ".............................................................. ....");
        //for (int i = cumSum.size() -1; i >= 0 ; i--){
        for (int i = 0; i < cumSum.size() ; i++){
            if (     x <= cumSum.get(i) ) {
                selectedIdx = i;
                return selectedIdx;
            }

        }


        return selectedIdx;

    }


    public static Double formateDoubleValue(Double value )
    {

        DecimalFormat df2 = new DecimalFormat("#.####");
        String formateed = df2.format(value);
        Double formatedValue = Double.parseDouble(formateed);
        return formatedValue;

    }


    public static void logger(String info)
    {

        FileWriter fw = null;
        BufferedWriter bw = null;
        PrintWriter pw = null;
        try {
            fw = new FileWriter("/home/ali/work/apache-storm-2.1.0/logs/Antcolony.log", true);
            bw = new BufferedWriter(fw);
            pw = new PrintWriter(bw);

            pw.println(info+"/r/n");

            pw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    public static void logInfo(String info)
    {
        Logger logger = Logger.getLogger("MyLog");
        FileHandler fh;

        try {
            System.out.println("..............................logInfo..................................................") ;
            // This block configure the logger with handler and formatter
            fh = new FileHandler("/home/ali/work/apache-storm-2.1.0/logs/antColony.log" , true );
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

            // the following statement is used to log any messages
            logger.info("My first log.......");


        } catch (SecurityException e) {
            //  System.out.println("..............................logInfo..................................................") ;
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("..............................logInfo..................................................") ;
            //      e.printStackTrace();
        }

        logger.info("Hi How r u?");


    }



    public static  List <ExecuterMetricsDetails> createTotalListComponentsInfo( Map <String , Map <String , Map<Double, Double>>> topologyCompponentsInfo, Cluster cluster )
    {
        /// this function creates total list from metrics information of components which can be used for selecting from them
        String topologyID ="";
        int port =0;
        String host = "";
        String SupervisorId ="";
        String WorkerSlot ="";
        String executers ="";
        Double capacity=0.0;
        Double executelatency=0.0;
        List <ExecuterMetricsDetails>  listexecuterTotalMetricsDetails = new  ArrayList<ExecuterMetricsDetails>();
        int cnt=0;
        for (Entry<String , Map <String , Map<Double, Double>>> entry : topologyCompponentsInfo.entrySet()) {
            cnt++;
            topologyID = entry.getKey();
            Map <String , Map<Double, Double>>  componenetsInfo = entry.getValue();
            for (Entry  <String , Map<Double, Double>>  entry2 : componenetsInfo.entrySet()) {

                String componet = entry2.getKey();  /// we should pars this string to extract port & host
                String[] arrSplit = componet.split("::");
                componet = arrSplit[0];
                host = arrSplit[1];
                port = Integer.parseInt(arrSplit[2]);
                executers = arrSplit[3];
                List<SupervisorDetails> lsthostToSupervisor = cluster.getSupervisorsByHost(host);
                for (SupervisorDetails hostToSupervisor : lsthostToSupervisor)
                {
                    SupervisorId= hostToSupervisor.getId();
                }
                Map<Double, Double> componentCapacityExecute =entry2.getValue();
                for (Entry  <Double, Double>  entry3 : componentCapacityExecute.entrySet()) {
                    ExecuterMetricsDetails executerMetricsDetails =new ExecuterMetricsDetails();
                    if (cnt == 1 ) {
                        capacity = 72.0;
                    }else
                    {
                        capacity = entry3.getKey();
                    }
                    executelatency = entry3.getValue();

                    executerMetricsDetails.setTopologyID(topologyID);
                    executerMetricsDetails.setSuperviorID(SupervisorId);
                    executerMetricsDetails.setComponent(componet);
                    executerMetricsDetails.setHost(host);
                    executerMetricsDetails.setPort(port);
                    executerMetricsDetails.setWorkerSlot(SupervisorId+":"+port);
                    executerMetricsDetails.setCapacity(capacity);
                    executerMetricsDetails.setExecuteLatency(executelatency);
                    executerMetricsDetails.setExecuters(executers);
                    // executerMetricsDetails.setExecuters("3-7");
                    listexecuterTotalMetricsDetails.add(executerMetricsDetails);
                }
            }
        }
        // System.out.println(":::::::::::::listexecuterTotalMetricsDetails:::::::::lst size::::::" +listexecuterTotalMetricsDetails.size() );

        return listexecuterTotalMetricsDetails;
    }






}
