package org.apache.storm.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Gabriele Scolastri on 15/09/15.
 */
public class ComponentMigrationChecker {

    public static Logger LOG = LoggerFactory.getLogger(ComponentMigrationChecker.class);

   // private static final String ZK_MIGRATION_DIR = "/extension/continuousscheduler/migrations";

    private static final String ZK_MIGRATION_DIR = "/home/ali/work/migration";

  //  private static final String ZK_MIGRATION_DIR = " /home/ali/work/zookeeper-3.5.5-bin/data";

    private List<String> localMigratingComponents = new ArrayList<String>();
    private SimpleZookeeperClient zkClient;

    public ComponentMigrationChecker(SimpleZookeeperClient client)
    {
        zkClient = client;
    }

    public void initialize()
    {
        // initialize zookeeper;
        System.out.println(".... ComponentMigrationChecker  initialize..00.");

        if (zkClient != null){
            try {
                zkClient.mkdirs(ZK_MIGRATION_DIR);
                System.out.println(".... ComponentMigrationChecker  initialize..11.");
                System.out.println(".... ComponentMigrationChecker  ZK_MIGRATION_DIR..."+ZK_MIGRATION_DIR);

            } catch (InterruptedException e) {
                System.out.println(".... ComponentMigrationChecker  error 00...");
                e.printStackTrace();
            } catch (org.apache.storm.shade.org.apache.zookeeper.KeeperException e) {
                System.out.println(".... ComponentMigrationChecker  error 11...");
                e.printStackTrace();
            }
        }

    }

    public boolean isMigrating(String topologyID, String componentID)
    {
        String dirname = ZK_MIGRATION_DIR + "/" + topologyID + "/" + componentID;

        boolean zkMigration = zkClient.exists(dirname);

        if (zkMigration){
            for(String localMigration : localMigratingComponents){
                /* If component is migrating by current node, don't consider its migration*/
                if (localMigration.equals(toMigrationID(topologyID, componentID))){
                    return false;
                }
            }

            return true;
        }else{
            return false;
        }
    }

    /* XXX: we should save the inode indicating {component, worker node id}, to guarantee no to "starve" if
     * a worker node fails and an executor needs to be moved */
    public void setMigration(String stormId, String componentId){
        ;
        String dirname = ZK_MIGRATION_DIR + "/" + stormId + "/" + componentId;
        String migrationId = toMigrationID(stormId, componentId);
        System.out.println("[GRADIENT] [TOPOLOGY] SET migration: " + migrationId);
        try {
            zkClient.mkdirs(dirname);

            localMigratingComponents.add(migrationId);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (org.apache.storm.shade.org.apache.zookeeper.KeeperException e) {
            e.printStackTrace();
        }
    }

    public void unsetMigration(String stormId, String componentId){

        String migrationId = toMigrationID(stormId, componentId);
        System.out.println("[GRADIENT] [TOPOLOGY] UNSET migration: " + migrationId);

        String dirname = ZK_MIGRATION_DIR + "/" + stormId + "/" + componentId;

        zkClient.deleteRecursive(dirname);

        localMigratingComponents.remove(migrationId);
    }

    private String toMigrationID(String topologyID, String componentID)
    {
        return topologyID + ":::" + componentID;
    }



}
