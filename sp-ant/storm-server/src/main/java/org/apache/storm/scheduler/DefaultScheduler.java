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

import org.apache.storm.utils.Utils;

import java.util.*;
import java.util.Map.Entry;

public class DefaultScheduler implements IScheduler {


    private static  boolean FirstRun = false;

    public static final  Topologies topologies= new Topologies();

    private static  Cluster mycluster = null;

    public static Double CpuUsage=0.0;

    //public static   Cluster cluster = null;



   // private static  boolean Firstrun = false;


    public class MyClass extends Thread {
        public void run(){
            System.out.println("MyClass running");
        }
    }






    private static Set<WorkerSlot> badSlots(Map<WorkerSlot, List<ExecutorDetails>> existingSlots, int numExecutors, int numWorkers) {
        if (numWorkers != 0) {
            Map<Integer, Integer> distribution = Utils.integerDivided(numExecutors, numWorkers);
            Set<WorkerSlot> slots = new HashSet<WorkerSlot>();

            for (Entry<WorkerSlot, List<ExecutorDetails>> entry : existingSlots.entrySet()) {
                Integer executorCount = entry.getValue().size();
                Integer workerCount = distribution.get(executorCount);
                if (workerCount != null && workerCount > 0) {
                    slots.add(entry.getKey());
                    workerCount--;
                    distribution.put(executorCount, workerCount);
                }
            }

            for (WorkerSlot slot : slots) {
                existingSlots.remove(slot);
            }

            return existingSlots.keySet();
        }

        return null;
    }

    public static Set<WorkerSlot> slotsCanReassign(Cluster cluster, Set<WorkerSlot> slots) {
        Set<WorkerSlot> result = new HashSet<WorkerSlot>();
        for (WorkerSlot slot : slots) {
            if (!cluster.isBlackListed(slot.getNodeId())) {
                SupervisorDetails supervisor = cluster.getSupervisorById(slot.getNodeId());
                if (supervisor != null) {
                    Set<Integer> ports = supervisor.getAllPorts();
                    if (ports != null && ports.contains(slot.getPort())) {
                        result.add(slot);
                    }
                }
            }
        }
        return result;
    }

    public static void defaultSchedule(Topologies topologies, Cluster cluster) {



        for (TopologyDetails topology : cluster.needsSchedulingTopologies()) {
            List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
            Set<ExecutorDetails> allExecutors = topology.getExecutors();

            Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned =
                    EvenScheduler.getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());
            Set<ExecutorDetails> aliveExecutors = new HashSet<ExecutorDetails>();
            for (List<ExecutorDetails> list : aliveAssigned.values()) {
                aliveExecutors.addAll(list);
            }

            Set<WorkerSlot> canReassignSlots = slotsCanReassign(cluster, aliveAssigned.keySet());
            int totalSlotsToUse = Math.min(topology.getNumWorkers(), canReassignSlots.size() + availableSlots.size());

            Set<WorkerSlot> badSlots = null;
            if (totalSlotsToUse > aliveAssigned.size() || !allExecutors.equals(aliveExecutors)) {
                badSlots = badSlots(aliveAssigned, allExecutors.size(), totalSlotsToUse);
            }
            if (badSlots != null) {
                cluster.freeSlots(badSlots);
            }

            EvenScheduler.scheduleTopologiesEvenly(new Topologies(topology), cluster);
        }






    }

    @Override
    public void prepare(Map<String, Object> conf) {
        //noop
    }

    public static void getWorkloadUsageInfo( Map<String, Map<String, String>> workloadUsageInfo) {




    }


    @Override
    public void schedule(Topologies topologies, Cluster cluster) {

        mycluster =cluster;


        defaultSchedule(topologies, mycluster);








        Collection<WorkerSlot> UsedSlots = cluster.getUsedSlots();
        int num_worker = UsedSlots.size();
       // System.out.println("::::::::::::Default Scheduler:::::::::::::::::::::" + UsedSlots.toString());


        //if (!FirstRun && (num_worker > 0)) {

        if (!FirstRun ) {




            Thread threadSubmitTopology = new Thread(new Runnable() {
                @Override
                public void run() {


                    BenchMarkSubmitter benchMarkSubmitter =new BenchMarkSubmitter();
                    benchMarkSubmitter.benchMarkSubmitter();

                }
            });



            Thread threadGetBenchMarkInfor = new Thread(new Runnable() {
                @Override
                public void run() {




                    BenchMarkGetInfo  benchMarkInfo= new BenchMarkGetInfo();
                    benchMarkInfo.benchMarkGetInfo();


                }
            });

            Thread threadGetExecuterDetails = new Thread(new Runnable() {
                @Override
                public void run() {




                    GetExecuterDetails  getExecuterDetails= new GetExecuterDetails();
                    getExecuterDetails.getExecuterDetailsDaemon();


                }
            });

            Thread threadHmetrics = new Thread(new Runnable() {
                @Override
                public void run() {




                    Hmetrics  hmetrics= new Hmetrics();
                    hmetrics.display_metrics(topologies,cluster);


                }
            });


            FirstRun=true;



        }









    }






    public static  Cluster getCluster() {
        return mycluster;
    }








    @Override
    public Map<String, Map<String, Double>> config() {
        return new HashMap<>();
    }



}