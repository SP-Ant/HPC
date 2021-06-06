/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.daemon.supervisor.timer;

import org.apache.storm.Config;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.scheduler.resource.normalization.NormalizedResources;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;

import java.util.*;

public class SupervisorHeartbeat implements Runnable {

    private final IStormClusterState stormClusterState;
    private final String supervisorId;
    private final Map<String, Object> conf;
    private final Supervisor supervisor;
    private static int interval=0;

    public SupervisorHeartbeat(Map<String, Object> conf, Supervisor supervisor) {
        this.stormClusterState = supervisor.getStormClusterState();
       // System.out.println("++++++++++++++++::SupervisorHeartbeat:::::: ++++++++++++++++"+stormClusterState);
        this.supervisorId = supervisor.getId();
        //System.out.println("++++++++++++++++::SupervisorHeartbeat:::::: ++++++++++++++++"+supervisorId);

        this.supervisor = supervisor;
        //System.out.println("++++++++++++++++::SupervisorHeartbeat:::::: ++++++++++++++++"+supervisor);
        this.conf = conf;
    }

    private SupervisorInfo buildSupervisorInfo(Map<String, Object> conf, Supervisor supervisor) {
        //System.out.println("++++++++++++++++::buildSupervisorInfo:::::SUPERVISOR_SCHEDULER_META: +++++interval+++++++++++"+interval);
//        interval++;
//
//
//        if (interval >= 5 ) {
//
//            interval =0;
//        }

        SupervisorInfo supervisorInfo = new SupervisorInfo();
        supervisorInfo.set_time_secs(Time.currentTimeSecs());
        supervisorInfo.set_hostname(supervisor.getHostName());
        supervisorInfo.set_assignment_id(supervisor.getAssignmentId());
        supervisorInfo.set_server_port(supervisor.getThriftServerPort());

        List<Long> usedPorts = new ArrayList<>();
        usedPorts.addAll(supervisor.getCurrAssignment().get().keySet());
        supervisorInfo.set_used_ports(usedPorts);
        List metaDatas = (List) supervisor.getiSupervisor().getMetadata();
        List<Long> portList = new ArrayList<>();
       // portList.add((long) 323.10) ;
        if (metaDatas != null) {
            for (Object data : metaDatas) {
                Integer port = ObjectReader.getInt(data);
                if (port != null) {
                    portList.add(port.longValue());
                }
            }
        }

        supervisorInfo.set_meta(portList);
       // supervisorInfo.set_meta("hhhh");

        Map<String, String>  metascheduler =  new HashMap<>();


        String cpuusageStr = SupervisorHealthCheck.CpuUsage.toString();
        String memoryStr = SupervisorHealthCheck.MemoryUsage.toString();

        //metascheduler.put("12.3","67.8");
        metascheduler.put(cpuusageStr,memoryStr);
       // metascheduler.put("memory","67");

       // System.out.println("++++++++++++++++::buildSupervisorInfo:::::SUPERVISOR_SCHEDULER_META: +++++before+++++++++++"+metascheduler);

        //supervisorInfo.set_scheduler_meta((Map<String, String>) conf.get(DaemonConfig.SUPERVISOR_SCHEDULER_META));
        supervisorInfo.set_scheduler_meta(metascheduler);

        supervisorInfo.set_uptime_secs(supervisor.getUpTime().upTime());
        supervisorInfo.set_version(supervisor.getStormVersion());
       // supervisorInfo.set_version("Version."+supervisor.getId());
        supervisorInfo.set_resources_map(mkSupervisorCapacities(conf));
        return supervisorInfo;
    }

    private Map<String, Double> mkSupervisorCapacities(Map<String, Object> conf) {
        Map<String, Double> ret = new HashMap<String, Double>();
        // Put in legacy values
        Double mem = ObjectReader.getDouble(conf.get(Config.SUPERVISOR_MEMORY_CAPACITY_MB), 4096.0);
        ret.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, mem);
        Double cpu = ObjectReader.getDouble(conf.get(Config.SUPERVISOR_CPU_CAPACITY), 400.0);
        ret.put(Config.SUPERVISOR_CPU_CAPACITY, cpu);


        // If configs are present in Generic map and legacy - the legacy values will be overwritten
        Map<String, Number> rawResourcesMap = (Map<String, Number>) conf.getOrDefault(
            Config.SUPERVISOR_RESOURCES_MAP, Collections.emptyMap()
        );

        for (Map.Entry<String, Number> stringNumberEntry : rawResourcesMap.entrySet()) {
            ret.put(stringNumberEntry.getKey(), stringNumberEntry.getValue().doubleValue());
        }

        return NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(ret);
    }

    @Override
    public void run() {
        SupervisorInfo supervisorInfo = buildSupervisorInfo(conf, supervisor);
        stormClusterState.supervisorHeartbeat(supervisorId, supervisorInfo);
    }
}
