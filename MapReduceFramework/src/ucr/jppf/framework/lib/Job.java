/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ucr.jppf.framework.lib;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jppf.JPPFException;
import org.jppf.client.JPPFClient;
import org.jppf.client.JPPFJob;
import org.jppf.node.protocol.Task;
import ucr.jppf.framework.Bigrama;

/**
 *
 * @author David
 */
public class Job {

    String name;
    Integer numReduceTasks;
    Integer numMapTasks;
    Class<? extends Mapper> mapperClass;
    Class<? extends Reducer> reducerClass;
    Class<? extends Reducer> combinerClass;
    Class<?> outputKeyClass;
    Class<?> outputValueClass;
    String outputPath;
    String inputPath;

    // JPPF stuff
    JPPFJob job;
    List<Task<?>> tasks;
    List<JPPFJob> jobs;

    private Job(String name) {
        this.name = name;
        job = new JPPFJob();
        job.setName(name);
        tasks = new ArrayList<>();
        jobs = new ArrayList<>();
    }

    public static Job getInstance(String name) {
        return new Job(name);
    }

    public void setMapperClass(Class<? extends Mapper> cls) {
        this.mapperClass = cls;
    }

    public void setReducerClass(Class<? extends Reducer> cls) {
        reducerClass = cls;
    }

    public void setCombinerClass(Class<? extends Reducer> cls) {
        this.combinerClass = cls;
    }

    public void setNumMapTasks(Integer numMapTasks) {
        this.numMapTasks = numMapTasks;
    }

    public void setNumReduceTasks(Integer numReduceTasks) {
        this.numReduceTasks = numReduceTasks;
    }

    public void setOutputKeyClass(Class<?> outputKeyClass) {
        this.outputKeyClass = outputKeyClass;
    }

    public void setOutputValueClass(Class<?> outputValueClass) {
        this.outputValueClass = outputValueClass;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    private void prepareJob() throws Exception {
        File file;
        FileChannel fileChannel;
        long size;
        long from = 0, to = 0;
        Task<?> task;

        file = new File(inputPath);
        fileChannel = new RandomAccessFile(file, "r").getChannel();
        System.out.println("size="+fileChannel.size()+" MAX_VALUE="+Integer.MAX_VALUE);
        size = fileChannel.size() > Integer.MAX_VALUE ? Integer.MAX_VALUE:fileChannel.size();
        System.out.println("size="+size);

        fileChannel.close();

        for (int i = 0; i < numMapTasks; i++) {
            to = from + size / numMapTasks;
            System.out.println(from + ":" + to);

            Mapper mapper = mapperClass.newInstance();
            mapper.setArch(inputPath);
            mapper.setBufferInit(from);
            mapper.setBufferLength(to);

            task = job.add(mapper);
            task.setId("mapper " + (i + 1));
            tasks.add(task);
            from = to;
        }
    }

    private void addReducer(final JPPFJob job, String name,
            HashMap<Bigrama, Integer> r1,
            HashMap<Bigrama, Integer> r2) throws Exception {
        Reducer reducer = reducerClass.newInstance();
        reducer.setHashMapLeft(r1);
        reducer.setHashMapRight(r2);
        Task<?> task = job.add(reducer);
        task.setId(name);
    }

    public void printHashMap(HashMap<Bigrama, Integer> resultado, String name) {
        System.out.println("\n" + name);
        for (Map.Entry<Bigrama, Integer> entry : resultado.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
        System.out.println("");
    }

    private void executeNonBlockingJob(final JPPFClient jppfClient) throws Exception {
        HashMap<Bigrama, Integer> resultado = new HashMap<>();
        HashMap<Bigrama, Integer> resultado2 = new HashMap<>();
        job.setBlocking(true);

        System.out.println("Submiting mappers");
        List<Task<?>> lista = jppfClient.submitJob(job);

        while (lista.size() > 1) {
            job = new JPPFJob(name);
            job.setBlocking(true);
            int c = 0;
            for (int i = 0; i < lista.size(); i = i + 2) {
                resultado = (HashMap<Bigrama, Integer>) lista.get(i).getResult();
                printHashMap(resultado, lista.get(i).getId());
                if (i + 1 < lista.size()) {
                    resultado2 = (HashMap<Bigrama, Integer>) lista.get(i + 1).getResult();
                    printHashMap(resultado2, lista.get(i + 1).getId());
                }
                addReducer(job, "reducer " + c++, resultado, resultado2);
                resultado2 = new HashMap<>();
            }
            System.out.println("\n\nSubmiting reducer");
            lista = jppfClient.submitJob(job);
            for (int i = 0; i < lista.size(); i++) {
                resultado = (HashMap<Bigrama, Integer>) lista.get(i).getResult();
                printHashMap(resultado, lista.get(i).getId());
            }
        }
    }

    public boolean waitForCompletion(boolean vervose) {

        try (JPPFClient jppfClient = new JPPFClient()) {
            prepareJob();
            executeNonBlockingJob(jppfClient);
        } catch (Exception ex) {
            Logger.getLogger(Job.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }
}
