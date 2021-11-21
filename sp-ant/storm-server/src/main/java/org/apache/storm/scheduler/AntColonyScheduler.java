package org.apache.storm.scheduler;

import org.apache.storm.generated.WorkerResources;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;  

public class AntColonyScheduler {

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



    public static void getTopologiesInfo(Cluster cluster, Topologies topologies) {
        System.out.println(".. Ant Colony scheduling....."  );
        Map<String, Map<String, Map<Double, Double>>> topologyCompponentsInfo = new HashMap<>();
        // topologyCompponentsInfo = GetExecuterDetails.getExecuterDetailsDaemon();
        //  System.out.println(".. Ant Colony ComponentsInfo.." +topologyCompponentsInfo );
        //System.out.println(".. Ant Colony getTopologiesInfo.."  );
        Collection<WorkerSlot> used_ports = cluster.getUsedSlots();

      //  if (used_ports.size() > 0) schedule(cluster, topologies);
    }

    public static void schedule(Cluster cluster, Topologies topologies) {

        /// set the initial scheduling.  We set it to false when we do not need initial scheduling

        boolean initialScheduling =false;
        if ( initialScheduling ) {
            RSPAntColonyScheduler.scheduleBinpacking(topologies, cluster);
        }

        boolean initialDone = initialEnvironment(cluster, topologies);
        System.out.println(".. Ant Colony schedule.." +initialDone );
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

        nAnt = 6;

        Q = 1;
        tau0 = 0.0;
        //  double  tau0=10*Q/(nVar*mean(model.D(:)));	% Initial Pheromone
      //  alpha = 1.0;//% Phromone Exponential Weight
        alpha = 2.0;
        beta = 1.0;       //  % Heuristic Exponential Weight
        rho = 0.05;     //  % Evaporation Rate
       // rho = 0.2;     //  % Evaporation Rate

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


 //       System.out.println(".................................................................");

        for (EtaHeuristicInformation etaHeuristicMatrix : etaHeuristicInformation) {
            if ( etaHeuristicMatrix.executerDetails.toString().equals(selectedExecuter.toString() ))
            {
                etaValue.put( etaHeuristicMatrix.workerSlot , etaHeuristicMatrix.value);
            }
        }


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
            fw = new FileWriter("/home/Antcolony.log", true);
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
           fh = new FileHandler("/home/Antcolony.log" , true );
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
