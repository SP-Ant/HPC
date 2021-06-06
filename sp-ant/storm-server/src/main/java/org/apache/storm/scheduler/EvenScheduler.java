/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler;

import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.shade.com.google.common.collect.Sets;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class EvenScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(EvenScheduler.class);
    public static boolean check_cpu_usae = false;
    public static boolean needToMigrate = false;

    private static SimpleZookeeperClient zkClient = null;
    //private SimpleZookeeperClient zkClient = null;

    private static ComponentMigrationChecker migrationTracker;
    private static  ISupervisor supervisor;




    @VisibleForTesting
    public static List<WorkerSlot> sortSlots(List<WorkerSlot> availableSlots) {
        //For example, we have a three nodes(supervisor1, supervisor2, supervisor3) cluster:
        //slots before sort:
        //supervisor1:6700, supervisor1:6701,
        //supervisor2:6700, supervisor2:6701, supervisor2:6702,
        //supervisor3:6700, supervisor3:6703, supervisor3:6702, supervisor3:6701
        //slots after sort:
        //supervisor3:6700, supervisor2:6700, supervisor1:6700,
        //supervisor3:6701, supervisor2:6701, supervisor1:6701,
        //supervisor3:6702, supervisor2:6702,
        //supervisor3:6703

        if (availableSlots != null && availableSlots.size() > 0) {
            // group by node
            Map<String, List<WorkerSlot>> slotGroups = new TreeMap<>();
            for (WorkerSlot slot : availableSlots) {
                String node = slot.getNodeId();
                List<WorkerSlot> slots = null;
                if (slotGroups.containsKey(node)) {
                    slots = slotGroups.get(node);
                } else {
                    slots = new ArrayList<WorkerSlot>();
                    slotGroups.put(node, slots);
                }
                slots.add(slot);
            }

            // sort by port: from small to large
            for (List<WorkerSlot> slots : slotGroups.values()) {
                Collections.sort(slots, new Comparator<WorkerSlot>() {
                    @Override
                    public int compare(WorkerSlot o1, WorkerSlot o2) {
                        return o1.getPort() - o2.getPort();
                    }
                });
            }

            // sort by available slots size: from large to small
            List<List<WorkerSlot>> list = new ArrayList<List<WorkerSlot>>(slotGroups.values());
            Collections.sort(list, new Comparator<List<WorkerSlot>>() {
                @Override
                public int compare(List<WorkerSlot> o1, List<WorkerSlot> o2) {
                    return o2.size() - o1.size();
                }
            });

            return ServerUtils.interleaveAll(list);
        }

        return null;
    }

    public static Map<WorkerSlot, List<ExecutorDetails>> getAliveAssignedWorkerSlotExecutors(Cluster cluster, String topologyId) {
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyId);
        Map<ExecutorDetails, WorkerSlot> executorToSlot = null;
        if (existingAssignment != null) {
            executorToSlot = existingAssignment.getExecutorToSlot();
        }

        return Utils.reverseMap(executorToSlot);
    }

    private static Map<ExecutorDetails, WorkerSlot> scheduleTopology(TopologyDetails topology, Cluster cluster) {
        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
        Set<ExecutorDetails> allExecutors = topology.getExecutors();
        Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());
        int totalSlotsToUse = Math.min(topology.getNumWorkers(), availableSlots.size() + aliveAssigned.size());

        List<WorkerSlot> sortedList = sortSlots(availableSlots);
        if (sortedList == null) {
            LOG.error("No available slots for topology: {}", topology.getName());
            return new HashMap<ExecutorDetails, WorkerSlot>();
        }

        //allow requesting slots number bigger than available slots
        int toIndex = (totalSlotsToUse - aliveAssigned.size())
                > sortedList.size() ? sortedList.size() : (totalSlotsToUse - aliveAssigned.size());
        List<WorkerSlot> reassignSlots = sortedList.subList(0, toIndex);

        Set<ExecutorDetails> aliveExecutors = new HashSet<ExecutorDetails>();
        for (List<ExecutorDetails> list : aliveAssigned.values()) {
            aliveExecutors.addAll(list);
        }
        Set<ExecutorDetails> reassignExecutors = Sets.difference(allExecutors, aliveExecutors);

        Map<ExecutorDetails, WorkerSlot> reassignment = new HashMap<ExecutorDetails, WorkerSlot>();
        if (reassignSlots.size() == 0) {
            return reassignment;
        }

        List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>(reassignExecutors);
        Collections.sort(executors, new Comparator<ExecutorDetails>() {
            @Override
            public int compare(ExecutorDetails o1, ExecutorDetails o2) {
                return o1.getStartTask() - o2.getStartTask();
            }
        });

        for (int i = 0; i < executors.size(); i++) {
            reassignment.put(executors.get(i), reassignSlots.get(i % reassignSlots.size()));
        }

        if (reassignment.size() != 0) {
            LOG.info("Available slots: {}", availableSlots.toString());
        }
        return reassignment;
    }

    public static void scheduleTopologiesEvenly(Topologies topologies, Cluster cluster) {

        for (TopologyDetails topology : cluster.needsSchedulingTopologies()) {
            String topologyId = topology.getId();
            Map<ExecutorDetails, WorkerSlot> newAssignment = scheduleTopology(topology, cluster);
            Map<WorkerSlot, List<ExecutorDetails>> nodePortToExecutors = Utils.reverseMap(newAssignment);



            for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : nodePortToExecutors.entrySet()) {
                WorkerSlot nodePort = entry.getKey();


                List<ExecutorDetails> executors = entry.getValue();
                List<ExecutorDetails> lstsourceexecuters = new ArrayList<ExecutorDetails>();
                List<ExecutorDetails> lstmovableexecuters = new ArrayList<ExecutorDetails>();

                for (ExecutorDetails lst : executors) {

                  //  System.out.println("++++++++++++++++::Even Scheduler:::::: +++++++lst++++executors+++++" + lst);

//                       if (!lst.toString().equals("[2, 2]") )
//                    //if (!lst.toString().equals("[3, 3]") && !lst.toString().equals("[4, 4]") )
//                     //      if (!lst.toString().equals("[12, 12]")  && !lst.toString().equals("[13, 13]") && !lst.toString().equals("[14, 14]") && !lst.toString().equals("[15, 15]") && !lst.toString().equals("[16, 16]") && !lst.toString().equals("[17, 17]") && !lst.toString().equals("[18, 18]") && !lst.toString().equals("[19, 19]") && !lst.toString().equals("[20, 20]") && !lst.toString().equals("[21, 21]") )
//                    {
//                        ExecutorDetails source =lst;
//                        lstsourceexecuters.add(source);
//
//                    }else
//                    {
//                        ExecutorDetails movable =lst;
//                        lstmovableexecuters.add(movable);
//
//                     }

                }


//                WorkerSlot avilablePort = null;
//                List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
//                for (WorkerSlot port : availableSlots) {
//                    // System.out.println("::::::::::::::::::::::::::::::::::::::::::::::::::avilablePorts::::::::::::00:::::::::::" + avilablePort);
//                    if (port.toString().contains("172.17.245.10")) {
//                        //System.out.println("::::::::::::::::::::::::::::::inside if::::::::::::::::::::avilablePorts::::::::::::00:::::::::::" + avilablePort);
//                        avilablePort = port;
//                       // nodePort =port;
//                       // System.out.println(":::::::::EvenScheduler:::::::::nodePort::::::::::11:::::::::::::" + avilablePort);
//                    }
//                }




                /// hamid


//                System.out.println("+++++++::Even Scheduler::topologyId:::++++" +topologyId);
//                System.out.println("+++++++::Even Scheduler:::nodePort::++++" + nodePort);
//                System.out.println("+++++++::Even Scheduler:::avilablePort::++++" + avilablePort);
//                System.out.println("+++++++::Even Scheduler:::executors::++++" + executors);
//                System.out.println("+++++++::Even Scheduler:::lstsourceexecuters::++++" + lstsourceexecuters);
//                System.out.println("+++++++::Even Scheduler:::lstmovableexecuters::++++" + lstmovableexecuters);

                //createZookeeperClient();

               // System.out.println("+++++++:Zookeeper++++"  );
                //supervisor = supervisor;
                //zkClient = zkClient;
               // System.out.println("+++++++:zkClient++++"  + zkClient.toString());

              //  migrationTracker = new ComponentMigrationChecker(zkClient);
              //  migrationTracker.initialize();

               // migrationTracker.unsetMigration(topology.getId(),"counter");


              //  migrationTracker.setMigration(topology.getId(), "logWriter");

               // migrationTracker.unsetMigration(topology.getId(), "counter");
               // System.out.println("+++++++::migration is set on zookeepr::done++++"  );


                //SchedulerAssignment assignment = cluster.getAssignmentById(topologyId);
               // System.out.println("+++before++++::assignment::++++" + assignment);


                cluster.assign(nodePort, topologyId, executors);
               // System.out.println("+++++++::assignment::done++++"  );







//                cluster.freeSlot(nodePort);
//                cluster.freeSlot(avilablePort);
////
//                cluster.assign(nodePort, topologyId, lstsourceexecuters);
//               cluster.assign(avilablePort, topologyId, lstmovableexecuters);
//
//                assignment = cluster.getAssignmentById(topologyId);
//                System.out.println("+++after++++::assignment::++++" + assignment);



            }
        }


    }


    @Override
    public void prepare(Map<String, Object> conf) {
        //noop
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {

        //if (!needToMigrate) {

         ///
//        System.out.println("................. Zookeeper......................");
//        this.supervisor = supervisor;
//        this.zkClient = zkClient;
//        this.migrationTracker = new ComponentMigrationChecker(zkClient);
//        migrationTracker.initialize();


        ///
            scheduleTopologiesEvenly(topologies, cluster);
        //}else
        //{
          //  needToMigrate =false;
        //}



    }

    @Override
    public Map<String, Map<String, Double>> config() {
        return new HashMap<>();
    }



    private static void createZookeeperClient(){

        /* Set default value */


        zkClient = null;
        System.out.println("................. calling createZookeeperClient......................");
        /* ZKClient can be created only if configuration file has been read correctly */
       // if (config != null){
            try {
                /* Retrieve ZK connection parameters */
//                Integer port = (Integer) config.get(Config.STORM_ZOOKEEPER_PORT);
//                Object obj = config.get(Config.STORM_ZOOKEEPER_SERVERS);

                Integer port = 2181;
                //Object obj = config.get(Config.STORM_ZOOKEEPER_SERVERS);

                //String servers ="localhost";

                //if (obj instanceof List){
                  // List<String> servers = (List<String>) obj;
                List<String> servers = new ArrayList<String>();
                servers.add ("localhost");

                    /* Create ZK client
                     * NOTE: connection is done asynchronously */
                    zkClient = new SimpleZookeeperClient(servers, port);

                    // DEBUG:
                    System.out.println("Connecting to ZooKeeper");
                    /* Initialization need to write to zookeeper. Wait until a connection is established */
                    while(!zkClient.isConnected()){
                        try {
                            // DEBUG:
                            System.out.print(".");
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    // DEBUG:
                    System.out.println();
                    System.out.println("ZkClient Created!");
               // }
            } catch (IOException e) {
                e.printStackTrace();
            }
       // }
    }





}
