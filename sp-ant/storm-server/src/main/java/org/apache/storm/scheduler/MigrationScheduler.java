package org.apache.storm.scheduler;


import java.util.*;

public class MigrationScheduler {



    private static ISupervisor supervisor;
    private static final String SYSTEM_COMPONENT_PREFIX = "__";
    private static boolean ContinueProgram=true;
    private static boolean oneTimeAssignment=false;
    public static List<ExecutorDetails> executorToAssignOnTotal = new ArrayList<ExecutorDetails>();
    public static Map<WorkerSlot ,List<ExecutorDetails>> result = new HashMap<>();

    public static void commitReturnMigration(Topologies topologies, Cluster cluster , String topolgyId,String component, List<ExecutorDetails> executorTotalOnSource ,WorkerSlot workerslotTarget ,WorkerSlot workerslotSource  ) {


//        System.out.println(":::::::::commitReturnMigration::::::sourcetopologyID::::" + topolgyId);
//        System.out.println(":::::::::commitReturnMigration:::::sourceComponent::::" + component);
//        System.out.println(":::::::::commitReturnMigration:::::targetNodeMigration::::" + workerslotTarget);
//        System.out.println(":::::::::commitReturnMigration:::sourceWorkerslot::::" + workerslotSource);
//        System.out.println(":::::::::commitReturnMigration:::::::::prepare:::executorToAssignOnTotal::::" + executorTotalOnSource);

        cluster.freeSlot(workerslotSource);
        cluster.freeSlot(workerslotTarget);
        //cluster.unassign(topolgyId);
        //cluster.unassign()

            cluster.assign(workerslotTarget, topolgyId, executorTotalOnSource);
        System.out.println("reverse migration is \"done\"");


    }



    public static Map<WorkerSlot ,List<ExecutorDetails>> commitExecuterMigration(Topologies topologies, Cluster cluster , String topolgyId,String component,String targetSupervisor , WorkerSlot workerslot  ) {

      //  System.out.println("                                                                     ");
      //  System.out.println("++++++++++++++++::MigrationScheduler::::schedule:::::: ++++++++++++++++");

        //boolean migrationDone =false;
        WorkerSlot migrationWorkerSlot =null;

//
//        Collection <WorkerSlot> workerslot =cluster.getUsedSlots();
//        //int num_worker = 0;
//        int num_worker = workerslot.size();
//        System.out.println("::::::::::num_worker::::::::::: "+num_worker);

//        if (num_worker > 0  && oneTimeAssignment == false)
//        {
//
//            oneTimeAssignment = true;

////
//            try
//            {
//                Thread.sleep(120000);
//            }
//            catch(InterruptedException ex)
//            {
//                Thread.currentThread().interrupt();
//            }
//
//            System.out.println("++++++++++++++++  after waiting ++++++++++++++++");
//
//
//           // return ;
//
//
//        }else
//        {
//            return;
//        }
//




        long start = System.currentTimeMillis();
       // System.out.println("[TIME]" +":"+ "" +  " STARTING SCHEDULING (" + start + ").");

        Map<String, SchedulerAssignment> assignments = cluster.getAssignments();


       // System.out.println("[TOPOLOGY] Processing " + assignments.keySet().size() + " topologies.");
      //  System.out.println("[TOPOLOGY] Processing assignments  00 " + assignments.toString());

        TopologyDetails topology;
        SchedulerAssignment assignment;

        String topologyID= topolgyId;

        // adaptation is done for each topology

       // for (String topologyID : assignments.keySet()) {

           // System.out.println( "[TOPOLOGY]:"  + " processing topology " + topologyID);

            // MONITOR
            // NOTE: sets migration notification on zookeeper for each exec.
            // As executors are being processed, their migration notification must be unset.
            //List<AugExecutorContext> moveableExecutors = getMoveableExecutorsContext(topologies.getById(topologyID), topologyContexts.get(topologyID), cluster.getAssignmentById(topologyID));
          //  System.out.println("[TOPOLOGY] Processing assignments  11 " );


            //cluster.setAssignments() ;
            List<AugExecutorContext> moveableExecutors = getMoveableExecutorsContext(cluster,topologies.getById(topologyID),  cluster.getAssignmentById(topologyID),component);



            topology = topologies.getById(topologyID);

        assignment = assignments.get(topologyID);




           // System.out.println("[MONITOR] [MOVEABLE] PRINTING MOVEABLE EXECUTORS in " + topology.toString());
          //  System.out.println("[MONITOR] [MOVEABLE] obtained " + moveableExecutors.size() + " executor(s)");
          //  System.out.println("[TOPOLOGY] Processing assignments  100 " );
          //  System.out.println(" processing topology 100 :::moveableExecutors  " + moveableExecutors.toString());




            for(AugExecutorContext exCtx : moveableExecutors) {

                WorkerSlot localSlot = exCtx.getAugExecutor().getWorkerSlot();
               // System.out.println("exCtx" +exCtx);
               // System.out.println("localSlot" +localSlot);


               // System.out.println("[MONITOR] [MOVEABLE] - " + (localSlot == null ? null : localSlot.getPort()) + " " + exCtx.getAugExecutor().getComponentId() + "(" + exCtx.getAugExecutor().getExecutor() + ")");
                List<ExecutorDetails> source = new ArrayList<ExecutorDetails>();
                List<ExecutorDetails> target = new ArrayList<ExecutorDetails>();
                System.out.println("before error 2 " );
                for (AugExecutorDetails aExec : exCtx.getNeighborExecutors()) {
                    if (exCtx.isTarget(aExec)) target.add(aExec.getExecutor());
                    else source.add(aExec.getExecutor());
                }
              //  System.out.println("[MONITOR] [MOVEABLE] - - Source: " + source);
              //  System.out.println("[MONITOR] [MOVEABLE] - - Target: " + target);

            }

             // System.out.println("localSlot  00" );

            for (AugExecutorContext executorContext : moveableExecutors) {
                //String nodeId = supervisor.getSupervisorId().toString();
                //hmd
              //  System.out.println("localSlot  11" );
                Map<String, SupervisorDetails> supervisordetls = cluster.getSupervisors();
                String nodeId="";
                for (  Map.Entry<String,  SupervisorDetails > entry : supervisordetls.entrySet()) {
                    nodeId = entry.getKey();
                }
                //hmd



                //int port =6701;
                WorkerSlot chosenWorkerslot= workerslot;


               // if (chosenWorkerslot == null || chosenWorkerslot.getNodeId().equals(supervisor.getSupervisorId())) {
                if (chosenWorkerslot == null ) {

                    continue;
                }






                boolean newAssignmentDone = commitNewExecutorAssignment(
                        chosenWorkerslot,
                        executorContext.getAugExecutor(),
                        topology,
                        cluster);

                if(newAssignmentDone) {
                    AugExecutorDetails ex = executorContext.getAugExecutor();
                    System.out.println("Migration done (sys time: " + System.currentTimeMillis() + ")");
                    System.out.println("Migrated " + ex.getExecutor() + " " + ex.getComponentId());
                    System.out.println("From     " + ex.getWorkerSlot());
                    System.out.println("To       " + chosenWorkerslot);

                   migrationWorkerSlot =   ex.getWorkerSlot() ;

                }

            }


       // } end for

        long end = System.currentTimeMillis();
        long elapsed = end - start;


         //return migrationWorkerSlot;
          return result;

        }





    private static boolean commitNewExecutorAssignment(
            WorkerSlot targetWorkerSlot,
            AugExecutorDetails executorToMigrate,
            TopologyDetails topology,
            Cluster cluster)

    {
        boolean CONFIG_DEBUG=true;

        SchedulerAssignment assignment = cluster.getAssignmentById(topology.getId());
        ExecutorDetails executorToMove = executorToMigrate.getExecutor();
        Map<ExecutorDetails, WorkerSlot> ex2ws = assignment.getExecutorToSlot();
      //  System.out.println("[MONITOR] [MOVEABLE] - - ex2ws: 103 " + ex2ws.toString());

        WorkerSlot sourceWorkerSlot = ex2ws.get(executorToMove);



        List<ExecutorDetails> executorToAssignOnSourceWS = new ArrayList<ExecutorDetails>(),
                executorToAssignOnTargetWS = new ArrayList<ExecutorDetails>();

        //List<ExecutorDetails> executorToAssignOnTotal = new ArrayList<ExecutorDetails>();
        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
        String targetslot= "";

//        for (WorkerSlot slot : availableSlots) {
//            String slot_node_id= slot.getNodeId();
//            //int slot_indx= slot.
//            System.out.println("   ******availableSlots******"+  (slot_node_id) );
//            if (slot_node_id.contains("172.17.245.11"))  targetWorkerSlot=  slot;
//        }




        for(ExecutorDetails executor : ex2ws.keySet())

        {


            executorToAssignOnTotal.add(executor) ;
            // executor to move must be assigned to target worker slot
            if(executor.equals(executorToMove)) {
                executorToAssignOnTargetWS.add(executor);
                continue;
            }

            WorkerSlot workerSlot = ex2ws.get(executor);


            if(workerSlot.equals(sourceWorkerSlot))
            {
                // once source worker slot is freed, executor must be reassigned
                executorToAssignOnSourceWS.add(executor);
                            }
            else if(workerSlot.equals(targetWorkerSlot))
            {
                // once target worker slot is freed, executor must be reassigned
                executorToAssignOnTargetWS.add(executor);
            }

            //executorToAssignOnTotal.add(executorToAssignOnTargetWS.ne) ;


        }


        if(CONFIG_DEBUG) {

//            System.out.println("[EXECUTE] executors to set on TARGET " + targetWorkerSlot);
//            System.out.println("[EXECUTE] - executorToAssignOnTargetWS  " + executorToAssignOnTargetWS.toString());
//            System.out.println("[EXECUTE] executors to set on LOCAL sourceWorkerSlot  " + sourceWorkerSlot);
//            System.out.println("[EXECUTE] - executorToAssignOnSourceWS  " + executorToAssignOnSourceWS.toString());
//            System.out.println("[EXECUTE] - topology.ID  " + topology.getId().toString());
//            System.out.println("[EXECUTE] committing changes ...");

        }


        try {
            // reassign executors to source worker slot

          //  System.out.println(" ....executorToAssignOnSourceWS..."+  (executorToAssignOnSourceWS) );
          //  System.out.println(" ....executorToAssignOnTargetWS..."  +  (executorToAssignOnTargetWS) );
          //  System.out.println(" ....executorToAssignOnTotal..."+  (executorToAssignOnTotal) );

//            for(ExecutorDetails executor : executorToAssignOnTargetWS)
//            {
//                executorToAssignOnTotal.add(executor);
//            }

          //  System.out.println(" ....executorToAssignOnTotal.11.."+  (executorToAssignOnTotal) );



            // cluster.unassign(topology.getId());

             cluster.freeSlot(sourceWorkerSlot);
           if (!executorToAssignOnSourceWS.isEmpty())
                cluster.assign(sourceWorkerSlot, topology.getId(), executorToAssignOnSourceWS);




           cluster.freeSlot(targetWorkerSlot);
           if (!executorToAssignOnTargetWS.isEmpty())
                cluster.assign(targetWorkerSlot, topology.getId(), executorToAssignOnTargetWS);

            result.put(sourceWorkerSlot,executorToAssignOnTotal);

        }
        catch (RuntimeException ex)
        {
            System.out.println("[EXECUTE] [RESULT] got exception " + ex.getClass().getSimpleName());
            ex.printStackTrace();
            return false;
        }
        catch (Exception ex)
        {
            System.out.println("[EXECUTE] [RESULT] got exception " + ex.getClass().getSimpleName());
            ex.printStackTrace();
            return false;
        }





        System.out.println("migration is \"done\"");
        return true;

    }



    //private List<AugExecutorContext> getMoveableExecutorsContext(TopologyDetails topology, GeneralTopologyContext context , SchedulerAssignment assignment)
    private static List<AugExecutorContext> getMoveableExecutorsContext(Cluster cluster,TopologyDetails topology,  SchedulerAssignment assignment,String component)
    {
        List<AugExecutorContext> moveableExecutors = new ArrayList<AugExecutorContext>();


        // preparing stuff
        if(assignment == null) return moveableExecutors;
        Map<ExecutorDetails, WorkerSlot> executorToWorkerSlot = assignment.getExecutorToSlot();

        if(executorToWorkerSlot == null || executorToWorkerSlot.isEmpty()) return moveableExecutors;
        Map<ExecutorDetails, WorkerSlot> localExecutorToWorkerSlot = new HashMap<ExecutorDetails, WorkerSlot>();

        //hmd
              // SupervisorDetails supervisor = cluster.getSupervisorById(slot.getNodeId());
             //  String supervisor_id = supervisor.getId() ;
           Map<String, SupervisorDetails> supervisordetls = cluster.getSupervisors();
           String localNodeID="";
           for (  Map.Entry<String,  SupervisorDetails > entry : supervisordetls.entrySet()) {

                localNodeID = entry.getKey();
           }

         //hmd




        // still preparing..
        for(ExecutorDetails exec : executorToWorkerSlot.keySet())
        {
            WorkerSlot wSlot = executorToWorkerSlot.get(exec);


            if(wSlot != null && localNodeID.equals(wSlot.getNodeId()))


                localExecutorToWorkerSlot.put(exec, wSlot);
        }

        boolean aRelatedComponentIsMigrating;
        boolean currentComponentIsLeafNode;
        boolean currentComponentIsRootNode;

        List<String> sourceComponents;
        List<String> targetComponents;
        for(ExecutorDetails localExecutor : localExecutorToWorkerSlot.keySet()) {
            /*	localExecutor cannot be moved if its corresponding localComponent:
             *  -	is System Component (null id or starts with "__")
             *  - 	is migrating already
             *  -   has source or target component which is migrating
             *  -	is a leaf node (all target are System Component)
             *  -	is a root node (all source are System Component)
             */
            String componentID = topology.getExecutorToComponent().get(localExecutor);


           // System.out.println(" processing topology 77 :::componentID  " + componentID);

            // avoid null components and pinned components
            if(componentID == null || componentID.startsWith(SYSTEM_COMPONENT_PREFIX))
                continue;

            AugExecutorContext augExecContext = new AugExecutorContext(new AugExecutorDetails(localExecutor, topology, assignment));
            //augExecContext.addNeighbors(topology, context, assignment);
           //  System.out.println(" processing topology 88 :::augExecContext  " + augExecContext.toString());

            ///hmd
            if (componentID.equals(component) == true) moveableExecutors.add(augExecContext);
            /// hmd



        }

        return moveableExecutors;
    }







    }




