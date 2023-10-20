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
import org.fog.utils.distribution.FireOnce;

import static org.fog.test.perfeval.OutputRedirector.*;
/**
 * Simulation setup for case study 1 - EEG Beam Tractor Game
 * @author Harshit Gupta
 *
 */
public class NewApproach {

    public static String getHelperId(int i){
        return "helper_device_" + i;
    }
    public static String getHelperClientModuleName(int i){
        return "helper_client_" + i;
    }

    public static int NUM_HELPERS = 2;
    public static int NUM_JOBS = 10;
    public static int FC_ID;
    public static String FC_CONTROLLER_DEVICE = "CONTROLLER";
    public static String FCModuleName = "ControllerModule";
    static List<FogDevice> fcControllers = new ArrayList<FogDevice>();
    static List<FogDevice> helperDevices = new ArrayList<FogDevice>();
    static List<Sensor> sensors = new ArrayList<Sensor>();
    static List<Actuator> actuators = new ArrayList<Actuator>();

    public static void main(String[] args) {
        int num_user = 1; // number of cloud user
        Calendar calendar = Calendar.getInstance();
        boolean trace_flag = false; // mean trace events
        Log.printLine("Running the tests.");
        try {
//            Log.disable();
            redirectOutputToFile(String.format("workable[%d]-N%d--J-%d.txt", System.currentTimeMillis() / (60 * 1000), NUM_HELPERS, NUM_JOBS));
            CloudSim.init(num_user, calendar, trace_flag);
            FogBroker mainBroker = new FogBroker("main");
            // DEVICES
            createInfrastructure();
            createHelpers(NUM_HELPERS);

            Application application = new Application("thisApp", 1);
            addJobs(application);
            ModuleMapping moduleMappings = createModuleMapping();

            createJobMetas(NUM_JOBS, 1, "thisApp");

            List<FogDevice> allFogDevices = getAllFogDevices();
            Controller simulatorController = new Controller("simulator_mega_node", allFogDevices, sensors, actuators);
            simulatorController.submitApplication(application, new ModulePlacementMapping(allFogDevices, application, moduleMappings));

            TimeKeeper.getInstance().setSimulationStartTime(Calendar.getInstance().getTimeInMillis());

            CloudSim.startSimulation();

//            CloudSim.stopSimulation();

//            Log.printLine("SingleFemtoCloud simulation finished!");
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }

    private static ModuleMapping createModuleMapping() {
        ModuleMapping mapping = ModuleMapping.createModuleMapping();
        mapping.addModuleToDevice(FCModuleName, FC_CONTROLLER_DEVICE);
        for (int i = 0; i < NUM_HELPERS; i++) {
            mapping.addModuleToDevice(getHelperClientModuleName(i), getHelperId(i));
        }
        return mapping;
    }

    private static List<FogDevice> getAllFogDevices() {
        return Stream.concat(fcControllers.stream(), helperDevices.stream()).collect(Collectors.toList());
    }


    private static void createInfrastructure() {
        // create backing cloud
        FogDevice cloud = createFogDevice("cloud", 44800, 40000, 100, 10000, 0, 0.01, 16*103, 16*83.25); // creates the fog device Cloud at the apex of the hierarchy with level=0
        cloud.setParentId(-1);
        // create the controller
        FogDevice fcController = createFogDevice(FC_CONTROLLER_DEVICE, 2800, 4000, 1000, 1000, 1, 0.0, 200, 83.4333); // creates the fog device Proxy Server (level=1)
        fcController.setParentId(cloud.getId()); // setting Cloud as parent of the Proxy Server
        fcController.setUplinkLatency(1000); // latency of connection from Proxy Server to the Cloud is 100 ms

        fcControllers.add(fcController);

        FC_ID = fcController.getId();
    }

    private static void createHelper(int id){
        FogDevice helperDevice = createFogDevice(getHelperId(id), 1000, 1000, 1000, 270, 2, 1,83.4333, 83);
        helperDevices.add(helperDevice);
        helperDevice.setParentId(FC_ID); // no parents, so no reuploading modueles
        helperDevice.setUplinkLatency(15);
    }

    private static void createHelpers(int helperNumber){
        for (int i = 0; i < helperNumber; i++) {
            createHelper(i);
        }
    }

    private static void createJobMeta(int id, int userId, String appId) {
        String sensor = getSensorName(id) + "_t";
        String jobInitiatorData = getSensorName(id) + "_t";
        Sensor jobSensor = new Sensor(sensor, jobInitiatorData ,userId, appId, new FireOnce(100*id));
        sensors.add(jobSensor);
        Actuator display = new Actuator(getDisplayNameId(id), userId, appId, getDisplayName(id));
        actuators.add(display);

        display.setGatewayDeviceId(FC_ID);
        display.setLatency(15.0);
        jobSensor.setGatewayDeviceId(FC_ID);
        jobSensor.setLatency(15.0);
    }

    private static String getSensorName(int id) {
        return "Initiator" + id;
    }

    private static String getDisplayNameId(int id) {
        return "Finalizer" + id;
    }

    private static String getDisplayName(int id) {
        return "Finalizer" + id + "_t";
    }


    private static void createJobMetas(int helperNumber, int userId, String appId){
        for (int i = 0; i < helperNumber; i++) {
            createJobMeta(i,userId,appId);
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

    private static Application addJobs(Application application){
        int phaseCount = 3;

        application.addAppModule(FCModuleName, 10);
        for (int i = 0; i < NUM_HELPERS; i++) {
            String helperIModule = getHelperClientModuleName(i);
            application.addAppModule(helperIModule, 10);
        }
        for (int j = 0; j < NUM_JOBS; j++) {

            String jobInitiatorData = getSensorName(j) + "_t";
            int scheduling_cpu = 2000;
            int task_details_NW = 100;
            application.addAppEdge(jobInitiatorData, FCModuleName, scheduling_cpu, task_details_NW, jobInitiatorData, Tuple.UP, AppEdge.SENSOR);

            String helperName = getHelperClientModuleName(job2helper(j));
            for (int phase = 0; phase < phaseCount; phase++) {
                  if (phase == 0){
                      application.addTupleMapping(FCModuleName, jobInitiatorData, packName(j, phase, false), new FractionalSelectivity(1.0));
                      application.addAppEdge(FCModuleName, helperName, 10000, 500, packName(j, phase, false), Tuple.DOWN, AppEdge.MODULE);
                  } else {
                      application.addAppEdge(FCModuleName, helperName, 10000, 500, packName(j, phase, false), Tuple.DOWN, AppEdge.MODULE);
                  }
                  application.addAppEdge(helperName, FCModuleName, 20000, 1000, packName(j, phase, true), Tuple.UP, AppEdge.MODULE);

//                Data -> Result edges
                  application.addTupleMapping(helperName, packName(j, phase, false), packName(j, phase, true), new FractionalSelectivity(1.0));
//                Result -> Data + 1
                  if (phase != phaseCount - 1){
                      application.addTupleMapping(FCModuleName, packName(j, phase, true), packName(j, phase  + 1, false), new FractionalSelectivity(1.0));
                  } else {
                      application.addTupleMapping(FCModuleName, packName(j, phase, true), getDisplayName(j), new FractionalSelectivity(1.0));
                      application.addAppEdge(FCModuleName, getDisplayName(j), 500, 500, getDisplayName(j), Tuple.DOWN, AppEdge.ACTUATOR);
                  }
            }
        }

        ArrayList<AppLoop> loops = new ArrayList<AppLoop>();
        for (int j = 0; j < NUM_JOBS; j++) {
            ArrayList<String> loopItems = new ArrayList<>();

            int helperI = job2helper(j);
            String jobInitiatorData = getSensorName(j) + "_t";
            String helperIModule = getHelperClientModuleName(helperI);
            String displayId = getDisplayName(j);
            loopItems.add(jobInitiatorData);
            loopItems.add(FCModuleName);
            loopItems.add(helperIModule);
            loopItems.add(FCModuleName);
            loopItems.add(displayId);

            loops.add(new AppLoop(loopItems));
        }
        application.setLoops(loops);

        return application;
    }

    private static String packName(int jobId, int phase, boolean isResult){
        String packageType = isResult ? "Result" : "Data";
        return String.format("j%d_p%d_%s", jobId, phase, packageType);
    }

    private static int job2helper(int job){
        return job % NUM_HELPERS;
    }

}