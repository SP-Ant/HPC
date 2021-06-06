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

package org.apache.storm.daemon.supervisor.timer;

import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.healthcheck.HealthChecker;
import org.apache.storm.scheduler.Hmetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class SupervisorHealthCheck implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SupervisorHealthCheck.class);
    private final Supervisor supervisor;

    public static Double CpuUsage=0.0;
    public static Double MemoryUsage=0.0;


    public SupervisorHealthCheck(Supervisor supervisor) {
        this.supervisor = supervisor;
    }

    @Override
    public void run() {
        Map<String, Object> conf = supervisor.getConf();
        LOG.info("Running supervisor healthchecks...");
        getUsageInfoFromHosts();
        //LOG.info(":::::::::::::hamid:::::::::::::::::::");
        int healthCode = HealthChecker.healthCheck(conf);
        if (healthCode != 0) {
            LOG.info("The supervisor healthchecks FAILED...");
            supervisor.shutdownAllWorkers(null, null);
            throw new RuntimeException("Supervisor failed health check. Exiting.");
        }
    }


    public static void getUsageInfoFromHosts() {


        ///
        //Double CpuUsage=0.0;
        //Double MemoryUsage=0.0;
       // System.out.println("++++++++++++++++::SupervisorHealthCheck:::::: +++++++getUsageInfoFromHosts+++++++++" );
        try
        {
            Runtime rt = Runtime.getRuntime();
            //  Process proc = rt.exec("dir");
            Process proc = rt.exec("top -b -n1");
            // Process proc = rt.exec("storm jar  /home/ali/work/apache-storm-2.1.0/bin/benchmark.jar  com.mamad.wordcount.Main AAA");

            InputStream stdin = proc.getInputStream();
            InputStreamReader isr = new InputStreamReader(stdin);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
           // System.out.println("<OUTPUT>");
            // LOG.info("<OUTPUT>");
            //System.out.println("<OUTPUT>");

            //br.re
            //String result="";
            int cnt=0;
            while ( (line = br.readLine()) != null)
            // System.out.println(line);
            {
//                result = result + line + "\t\n";
//                cnt++;
//                if (cnt > 5) break;

                if (line.startsWith("top") ){


                    String result = line.substring(line.indexOf("load average:") , line.length());
                    result=result.replace("load average:","");

                    String[] arrOfStr = result.split(",");
                    String tmp=arrOfStr[1].toString();
                    CpuUsage= Double.parseDouble(tmp);
                    //System.out.println("::cpuusage::IP"+ IP + " :::" + CpuUsage);




                }

                if (line.startsWith("KiB Swap") ) {
                    String result = line.substring(line.indexOf("used"), line.length());
                    result = result.replace("used.", "");
                    result = result.replace("avail Mem", "");
                    result = result.trim();

                    MemoryUsage= Double.parseDouble(result);
                    //System.out.println("::MemoryUsage::+IP" + IP +  ":::::" + MemoryUsage);
                }


            }

          //  System.out.println(":::::::::Functioin:::::::::CPU::::::::" + CpuUsage);
          //  System.out.println(":::::::::Functioin:::::::::MemoryUsage::::::::" + MemoryUsage);

            Hmetrics.CpuUsage= CpuUsage;
           Hmetrics.MemoryUsage= MemoryUsage;

            //System.out.println(":::::::::Functioin:::::::::Hmetrics.CpuUsage::::::::" + Hmetrics.CpuUsage);
            //System.out.println(":::::::::Functioin:::::::::Hmetrics.MemoryUsage::::::::" + Hmetrics.MemoryUsage);

            // Hmetrics hmetrics= new Hmetrics();
             ///hmetrics.setcpuusage(CpuUsage);

            //DefaultScheduler.CpuUsage=CpuUsage;

            //result ="";


            //System.out.println("</OUTPUT>");
            int exitVal = proc.waitFor();
            // System.out.println("Process exitValue: " + exitVal);

        } catch (Throwable t)
        {
            t.printStackTrace();
        }




        ///

    }


}
