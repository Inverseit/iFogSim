package org.fog.test.perfeval;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.sdn.overbooking.BwProvisionerOverbooking;
import org.cloudbus.cloudsim.sdn.overbooking.PeProvisionerOverbooking;
import org.fog.application.AppEdge;
import org.fog.application.AppLoop;
import org.fog.application.Application;
import org.fog.application.selectivity.FractionalSelectivity;
import org.fog.entities.Actuator;
import org.fog.entities.FogBroker;
import org.fog.entities.FogDevice;
import org.fog.entities.FogDeviceCharacteristics;
import org.fog.entities.Sensor;
import org.fog.entities.Tuple;
import org.fog.placement.Controller;
import org.fog.placement.ModuleMapping;
import org.fog.placement.ModulePlacementMapping;
import org.fog.policy.AppModuleAllocationPolicy;
import org.fog.scheduler.StreamOperatorScheduler;
import org.fog.utils.FogLinearPowerModel;
import org.fog.utils.FogUtils;
import org.fog.utils.TimeKeeper;
import org.fog.utils.distribution.DeterministicDistribution;

import static org.fog.test.perfeval.OutputRedirector.*;
/**
 * Simulation setup for case study 1 - EEG Beam Tractor Game
 * @author Harshit Gupta
 *
 */
public class SingleFemtoCloudTests {

    public static String getJobId(int i){
        return "job_" + i;
    }

    public static String getHelperId(int i){
        return "helper_" + i;
    }

    public static int NUM_HELPERS = 3;
    public static int NUM_JOBS = 6;
    public static int FC_ID;
    public static String FC_CONTROLLER = "CONTROLLER";
    public static String FCName = "ControllerModule";
    static List<FogDevice> fcControllers = new ArrayList<FogDevice>();
    static List<FogDevice> mobiles = new ArrayList<FogDevice>();
    public static void main(String[] args) {

        Log.printLine("Starting SingleFemtoCloud Tests...");

        try {
//            Log.disable();
            redirectOutputToFile("ulan2.txt");
            int num_user = 1; // number of cloud user
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false; // mean trace events

            CloudSim.init(num_user, calendar, trace_flag);


            FogBroker mainBroker = new FogBroker("main");

            // DEVICES
            createInfrastructure();
            createHelpers(NUM_HELPERS);


            // JOBS
            List<Application> jobs = createJobs(NUM_JOBS, mainBroker.getId());
            // adding delays manually
            List<Integer> delays = new ArrayList<>();
            for (int i = 0; i < NUM_JOBS; i++) {
                delays.add(200 * i);
            }

            ModuleMapping moduleMappings = createModuleMappings(jobs);
            List<FogDevice> allFogDevices = getAllFogDevices();
            Controller simulatorController = new Controller("simulator_mega_node", allFogDevices, new ArrayList<Sensor>(), new ArrayList<Actuator>());

            for (int i = 0; i < NUM_JOBS; i++) {
                int delay = delays.get(i);
                Application job = jobs.get(i);
                simulatorController.submitApplication(job, delay, new ModulePlacementMapping(allFogDevices, job, moduleMappings));
            }

            TimeKeeper.getInstance().setSimulationStartTime(Calendar.getInstance().getTimeInMillis());

            CloudSim.startSimulation();

            CloudSim.stopSimulation();

            Log.printLine("SingleFemtoCloud simulation finished!");
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }

    private static ModuleMapping createModuleMappings(List<Application> jobs) {
        ModuleMapping moduleMapping = ModuleMapping.createModuleMapping(); // initializing a module mapping
        for (int i = 0; i < jobs.size(); i++) {
            Application job = jobs.get(i);
            List<String> modules = job.getModuleNames();
            String assignedHelperName = getHelperId(i % NUM_HELPERS);
            for (String module: modules) {
                if (module.startsWith("ControllerModule")){
                    moduleMapping.addModuleToDevice(module, FC_CONTROLLER);
                } else if (module.startsWith("part")){
                    moduleMapping.addModuleToDevice(module, assignedHelperName);
                }
                else {
                    throw new NoSuchElementException("No such module:" + module);
                }
            }

        }
        return moduleMapping;
    }

    private static List<FogDevice> getAllFogDevices() {
        return Stream.concat(mobiles.stream(), fcControllers.stream()).collect(Collectors.toList());
    }

    private static void createInfrastructure() {
        // create backing cloud
        FogDevice cloud = createFogDevice("cloud", 44800, 40000, 100, 10000, 0, 0.01, 16*103, 16*83.25); // creates the fog device Cloud at the apex of the hierarchy with level=0
        cloud.setParentId(-1);
        // create the controller
        FogDevice fcController = createFogDevice(FC_CONTROLLER, 2800, 4000, 1000, 1000, 1, 0.0, 107.339, 83.4333); // creates the fog device Proxy Server (level=1)
        fcController.setParentId(cloud.getId()); // setting Cloud as parent of the Proxy Server
        fcController.setUplinkLatency(1000); // latency of connection from Proxy Server to the Cloud is 100 ms

        fcControllers.add(cloud);
        fcControllers.add(fcController);

        FC_ID = fcController.getId();
    }

    private static void createHelper(int id){
        FogDevice helperDevice = createFogDevice(getHelperId(id), 1000, 1000, 1000, 270, 2, 0, 87.53, 82.44);
        mobiles.add(helperDevice);
        helperDevice.setParentId(-1);
        helperDevice.setUplinkLatency(100); // latency of connection between this device and controller server is 10 ms
    }

    private static void createHelpers(int helperNumber){
        for (int i = 0; i < helperNumber; i++) {
           createHelper(i);
        }
    }

    /**
     * Creates a vanilla fog device
     * @param nodeName name of the device to be used in simulation
     * @param mips MIPS
     * @param ram RAM
     * @param upBw uplink bandwidth
     * @param downBw downlink bandwidth
     * @param level hierarchy level of the device
     * @param ratePerMips cost rate per MIPS used
     * @param busyPower
     * @param idlePower
     * @return
     */
    private static FogDevice createFogDevice(String nodeName, long mips,
                                             int ram, long upBw, long downBw, int level, double ratePerMips, double busyPower, double idlePower) {

        List<Pe> peList = new ArrayList<Pe>();

        // 3. Create PEs and add these into a list.
        peList.add(new Pe(0, new PeProvisionerOverbooking(mips))); // need to store Pe id and MIPS Rating

        int hostId = FogUtils.generateEntityId();
        long storage = 1000000; // host storage
        int bw = 10000;

        PowerHost host = new PowerHost(
                hostId,
                new RamProvisionerSimple(ram),
                new BwProvisionerOverbooking(bw),
                storage,
                peList,
                new StreamOperatorScheduler(peList),
                new FogLinearPowerModel(busyPower, idlePower)
        );

        List<Host> hostList = new ArrayList<Host>();
        hostList.add(host);

        String arch = "x86"; // system architecture
        String os = "Linux"; // operating system
        String vmm = "Xen";
        double time_zone = 10.0; // time zone this resource located
        double cost = 3.0; // the cost of using processing in this resource
        double costPerMem = 0.05; // the cost of using memory in this resource
        double costPerStorage = 0.001; // the cost of using storage in this
        // resource
        double costPerBw = 0.0; // the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are not adding SAN devices by now

        FogDeviceCharacteristics characteristics = new FogDeviceCharacteristics(
                arch, os, vmm, host, time_zone, cost, costPerMem,
                costPerStorage, costPerBw);

        FogDevice fogdevice = null;
        try {
            fogdevice = new FogDevice(nodeName, characteristics,
                    new AppModuleAllocationPolicy(hostList), storageList, 10, upBw, downBw, 0, ratePerMips);
        } catch (Exception e) {
            e.printStackTrace();
        }

        fogdevice.setLevel(level);
        return fogdevice;
    }

    private static Application createJob(String appId, int userId){

        Application application = Application.createApplication(appId, userId); // creates an empty application model (empty directed graph)

        /*
         * Adding modules (vertices) to the application model (directed graph)
         */
        String suffix = "-" + appId;
//        String FCName = "ControllerModule" + suffix;
        application.addAppModule("part0" + suffix, 10); // adding module Client to the application model
        application.addAppModule("part1" + suffix, 10); // adding module Connector to the application model
        application.addAppModule(FCName, 10); // adding module Connector to the application model

        application.addAppEdge(FCName, "part0" + suffix,1234, 100, "Data0" + suffix, Tuple.DOWN, AppEdge.MODULE);
        application.addAppEdge("part0", FCName, 1234, 100, "Result0" + suffix, Tuple.UP, AppEdge.MODULE);
        application.addAppEdge(FCName, "part1"+ suffix, 1234, 100, "Data1" + suffix, Tuple.DOWN, AppEdge.MODULE);
        application.addAppEdge("part1" + suffix, FCName, 12345, 100, "Result1" + suffix, Tuple.UP, AppEdge.MODULE);

        /*
         * Defining the input-output relationships (represented by selectivity) of the application modules.
         */
        application.addTupleMapping("part0" + suffix, "Data0" + suffix, "Result0" + suffix, new FractionalSelectivity(1.0)); // 0.9 tuples of type _SENSOR are emitted by Client module per incoming tuple of type EEG
        application.addTupleMapping(FCName, "Result0" + suffix, "Data1" + suffix, new FractionalSelectivity(1.0)); // 1.0 tuples of type SELF_STATE_UPDATE are emitted by Client module per incoming tuple of type CONCENTRATION
        application.addTupleMapping("part1" + suffix, "Data1" + suffix, "Result1" + suffix, new FractionalSelectivity(1.0)); // 0.9 tuples of type _SENSOR are emitted by Client module per incoming tuple of type EEG
        //TODO: Add a display to make sure that final result is also calculated
        //application.addAppEdge("client", "DISPLAY", 1000, 500, "GLOBAL_STATE_UPDATE", Tuple.DOWN, AppEdge.ACTUATOR);  // adding edge from Client module to Display (actuator) carrying tuples of type GLOBAL_STATE_UPDATE
        /*
         * Defining application loops to monitor the latency of.
         * Here, we add only one loop for monitoring : EEG(sensor) -> Client -> Concentration Calculator -> Client -> DISPLAY (actuator)
         */
        List<String> loopItems = new ArrayList<>();
        loopItems.add(FCName);
        loopItems.add("part0" + "-" + appId);
        loopItems.add(FCName);
        loopItems.add("part1" + "-" + appId);
        loopItems.add(FCName);

        final AppLoop mainLoop = new AppLoop(loopItems);
        List<AppLoop> loops = new ArrayList<>();
        loops.add(mainLoop);

        application.setLoops(loops);

        return application;
    }

    private static List<Application> createJobs(int jobCount, int userId){
        List<Application> jobs = new ArrayList<Application>();
        for (int i = 0; i < jobCount; i++) {
            jobs.add(createJob(getJobId(i), userId));
        }
        return jobs;
    }
}