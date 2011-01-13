/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.benchmark.workloads;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.ClientMain;
import org.voltdb.benchmark.workloads.paramgen.GeneratorFactory;
import org.voltdb.benchmark.workloads.paramgen.ParamGenerator;
import org.voltdb.benchmark.workloads.xml.LoaderType;
import org.voltdb.benchmark.workloads.xml.Microbenchmark;
import org.voltdb.benchmark.workloads.xml.ParamType;
import org.voltdb.benchmark.workloads.xml.ProcedureType;
import org.voltdb.benchmark.workloads.xml.WorkloadType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

//CLASSPATH=/home/mstarobinets/voltbin/mysql.jar  ant benchmarkcluster -Dclient=org.voltdb.benchmark.workloads.Generator -Dworkload=MixedLoad -Dhostcount=4 -Dclientcount=2 -Dhost1=volt3i -Dhost2=volt3j -Dhost3=volt3k -Dhost4=volt3l -Dclienthost1=volt1 -Dclienthost2=volt4a
//COMMAND: xjc -p benchmarkGenerator.xml /home/voltdb/mstarobinets/Desktop/Useful/MB/microbenchmark1.xsd -d /home/voltdb/mstarobinets/Desktop/Useful/MB
public class Generator extends ClientMain
{
    public class Workload
    {
        private final String name;
        private String[] procs;
        private double[] percs;
        private ParamGenerator[][] paramGenerators;
        private Object[][] params;

        public Workload(String name)
        {
            this.name = name;
        }
    }

    public class GenericCallback implements ProcedureCallback
    {
        int m_procIndex;

        public GenericCallback(int procIndex)
        {
            m_procIndex = procIndex;
        }

        public void clientCallback(ClientResponse clientResponse)
        {
            //do error checking
            m_counts[m_procIndex].getAndIncrement();
        }
    }

    //Retrieved via reflection by BenchmarkController
    public static Class<? extends WorkloadProjectBuilder> m_projectBuilderClass = ProjectBuilderX.class;
    public static WorkloadProjectBuilder m_projectBuilder = null;

    //Retrieved via reflection by BenchmarkController
    //public static final Class<? extends ClientMain> m_loaderClass = anyLoader.class;
    public static final Class<? extends ClientMain> m_loaderClass = null;

    private Microbenchmark mb;
    private LinkedList<Workload> workloads;
    private static String workloadToBuild;
    private Workload currWorkload;

    private static String xmlFilePath;
    private boolean built;

    public Generator(String[] args)
    {
        super(args);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args)
    {
        //xmlFilePath = "../workspace/voltdb/tests/frontend/org/voltdb/benchmark/workloads/microbench.xml";
        xmlFilePath = null;
        workloadToBuild = null;
        for (int i = 0; i < args.length; i++)
        {
            if (args[i].startsWith("configfile="))
            {
                //check that path is valid using getXMLFile() method?
                xmlFilePath = args[i].split("=")[1];
            }
            else if (args[i].startsWith("workload="))
            {
                workloadToBuild = args[i].split("=")[1];
            }
            else if (args[i].startsWith("PROJECTBUILDERNAME="))
            {
                String pbName = args[i].split("=")[1];
                try
                {
                    m_projectBuilderClass =
                        (Class<? extends WorkloadProjectBuilder>)Class.forName(pbName);
                }
                catch (ClassNotFoundException e)
                {
                    System.err.println("Unable to find VoltProjectBuilder class: " + pbName);
                    System.exit(-1);
                }
            }
        }

        try
        {
            m_projectBuilder = m_projectBuilderClass.newInstance();
        }
        catch (InstantiationException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
        catch (IllegalAccessException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }

        ClientMain.main(Generator.class, args, false);
    }

    private void runBenchmark()
    {
        if (built)
        {
            System.err.println("About to run workload " + currWorkload.name);
            runRandomizedWorkload(currWorkload);
        }
    }

    public void runRandomizedWorkload(Workload currWorkload)
    {
        //if procArray.length != percArray.length throw error?
        if (!(currWorkload.procs.length == currWorkload.percs.length && currWorkload.percs.length == currWorkload.params.length))
        {
            System.err.println("Invalid inputs in xml file for workload \"" + currWorkload.name + "\".");
            return;
        }

        if (currWorkload.procs.length > 0)
            //throw new Exception();
        {
            double[] cumPercArray = new double[currWorkload.percs.length];
            cumPercArray[0] = currWorkload.percs[0];
            for (int i = 1; i < currWorkload.percs.length; i++)
                cumPercArray[i] = cumPercArray[i - 1] + currWorkload.percs[i];

            //ADDING DOUBLES IS NOT PRECISE! EDIT THIS...
            if (cumPercArray[cumPercArray.length - 1] != 100.)
            {
                System.err.println("Invalid inputs in xml file for workload \"" + currWorkload.name + "\".");
                return;
            }

            double randomVal;
            while (true)
            {
                randomVal = Math.random() * 100;
                int index = 0;
                while (randomVal > cumPercArray[index])
                //&& while array index not out of bounds
                    index++;

                for (int i = 0; i < currWorkload.params[index].length; i++)
                    setParams(currWorkload, index, i);
                callProc(currWorkload.procs[index], currWorkload.params[index], index);
            }
        }
    }

    // This wasn't used. Can be deleted?
    /*private long getTotalTime()
    {
        long nanoTime = 0;

        Scanner scan;
        boolean inputValid = false;
        while (!inputValid)
        {
            System.err.print("Enter duration of workload with units in descending order (ex. 1h2m3s4n, 3m 1n, etc.): ");
            scan = new Scanner(System.in);
            String input = scan.nextLine();
            //do try catch, negative values...
            if (input.matches("(\\d+\\s*h)?\\s*(\\d+\\s*m)?\\s*(\\d+\\s*s)?\\s*(\\d+\\s*n)?\\s*") && !input.trim().equals(""))
            {
                try
                {
                    input = input.replaceAll("\\s", "");

                    if (input.contains("h"))
                    {
                        int index = input.indexOf('h');
                        long value = Integer.parseInt(input.substring(0, index));
                        if ((Long.MAX_VALUE - nanoTime) / 1000000000. / 60 / 60 < value)
                        {
                            System.err.println("Value exceeds limit of about 2.5 million total hours");
                            throw new Exception();
                        }
                        else
                        {
                            nanoTime += value * 1000000000 * 60 * 60;
                            input = input.substring(index + 1);
                        }
                    }
                    if (input.contains("m"))
                    {
                        int index = input.indexOf('m');
                        long value = Integer.parseInt(input.substring(0, index));
                        if ((Long.MAX_VALUE - nanoTime) / 1000000000. / 60 < value)
                        {
                            System.err.println("Value exceeds limit of about 150 million total minutes");
                            throw new Exception();
                        }
                        else
                        {
                            nanoTime += value * 1000000000 * 60;
                            input = input.substring(index + 1);
                        }
                    }
                    if (input.contains("s"))
                    {
                        int index = input.indexOf('s');
                        long value = Integer.parseInt(input.substring(0, index));
                        if ((Long.MAX_VALUE - nanoTime) / 1000000000. < value)
                        {
                            System.err.println("Value exceeds limit of about 9 billion total seconds");
                            throw new Exception();
                        }
                        else
                        {
                            nanoTime += value * 1000000000;
                            input = input.substring(index + 1);
                        }
                    }
                    if (input.contains("n"))
                    {
                        int index = input.indexOf('n');
                        long value = Integer.parseInt(input.substring(0, index));
                        if ((Long.MAX_VALUE - nanoTime) < value)
                        {
                            System.err.println("Value exceeds limit of about 9 quintillion total nanoseconds");
                            throw new Exception();
                        }
                        else
                        {
                            nanoTime += value;
                            input = input.substring(index + 1);
                        }
                    }

                    inputValid = true;
                }
                catch (Exception e)
                {
                    System.err.println("Parse error.");
                }
            }
            else
            {
                System.err.println("Regex error.");
            }
        }

        return nanoTime;
    }*/

    private void callProc(String procName, Object[] params, int procIndex)
    {
        //check procName validity with catch statement

        GenericCallback callback = new GenericCallback(procIndex);

        try
        {
            while (!m_voltClient.callProcedure(callback, procName, params))
            {
                try {
                    m_voltClient.backpressureBarrier();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        /*
        catch (ProcCallException e)
        {
            System.err.println("Procedure call error: " + e.getMessage());
        }
        */
    }

    @Override
    protected void runLoop()
    {
        try
        {
            runBenchmark();
        }
        catch (Exception e)
        {
            return;
        }
    }

    @Override
    protected String[] getTransactionDisplayNames()
    {
        workloads = new LinkedList<Workload>();

        File xmlFile = null;

        if (xmlFilePath == null)
        {
            InputStream iStream = this.getClass().getResourceAsStream("microbench.xml");
            mb = unmarshal(iStream);
        }
        else
        {
            xmlFile = new File(xmlFilePath);
            mb = unmarshal(xmlFile);
        }

        boolean loaded = runLoader(mb);
        testPrint();

        if (!loaded)
            System.err.println("No loading took place.");
        if (!buildWorkload(mb, workloadToBuild))
        {
            built = false;
            System.err.println("Building workload + " + workloadToBuild + " failed.");
            return null;
        }
        else
        {
            built = true;
            currWorkload = workloads.getFirst();
            String[] displayNames = new String[currWorkload.procs.length];
            for (int i = 0; i < displayNames.length; i++)
                displayNames[i] = currWorkload.procs[i];
            return displayNames;
            /*
            //FOR MULTIPLE WORKLOADS:
            int numProcs = 0;
            for (Workload w : workloads)
                numProcs += w.procs.length;
            String[] displayNames = new String[numProcs];
            int workloadNum = 0;
            int index = 0;
            for (Workload w : workloads)
            {
                for (int i = 0; i < w.procs.length; i++)
                {
                    displayNames[index] = "WL" + workloadNum + ": " + w.name + " - Proc" + i + ": " + w.procs[i];
                    index++;
                }
                workloadNum++;
            }
            return displayNames;
            */
        }
    }

    @Override
    protected String getApplicationName()
    {
        return "Microbenchmark";
    }

    @Override
    protected String getSubApplicationName()
    {
        if (workloadToBuild != null)
            return workloadToBuild;
        else
            return "FirstWorkload";
    }

    //ADD FEEDBACK: PRINTOUTS/FILEWRITES ABOUT CREATED WORKLOADS
    private boolean buildWorkload(Microbenchmark mb, String workloadToBuild)
    {
        if (mb == null)
            return false;

        try
        {
            List<WorkloadType> wlList = mb.getWorkload();
            WorkloadType toBuild = wlList.get(0);
            boolean foundWL = false;

            if (workloadToBuild != null)
            {
                ListIterator<WorkloadType> wlLI = wlList.listIterator();
                while (wlLI.hasNext())
                {
                    WorkloadType wl = wlLI.next();
                    if (wl.getWlName().equals(workloadToBuild))
                    {
                        toBuild = wl;
                        foundWL = true;
                        break;
                    }
                }
            }

            if (!foundWL)
            {
                System.err.println("***************************************************************************************************");
                System.err.println("|                                                                                                 |");
                System.err.println("|    Specified workload not found in config file. Defaulting to first workload of config file.    |");
                System.err.println("|                                                                                                 |");
                System.err.println("***************************************************************************************************");
            }

            Workload myWL = new Workload(toBuild.getWlName());

            System.err.println("Building workload " + myWL.name);

            List<ProcedureType> procList = toBuild.getProcedure();
            myWL.procs = new String[procList.size()];
            myWL.percs = new double[procList.size()];
            myWL.paramGenerators = new ParamGenerator[procList.size()][];
            myWL.params = new Object[procList.size()][];

            ListIterator<ProcedureType> procLI = procList.listIterator();
            int procIndex = 0;
            while (procLI.hasNext())
            {
                ProcedureType proc = procLI.next();
                myWL.procs[procIndex] = proc.getProcName();
                myWL.percs[procIndex] = proc.getPercOfWL().doubleValue();

                Class<?> proc_class = null;
                for (Class<?> clazz : m_projectBuilder.getProcedures())
                {
                    if (clazz.getSimpleName().equals(proc.getProcName()))
                    {
                        proc_class = clazz;
                        break;
                    }
                }
                if (proc_class == null)
                {
                    System.err.println("Stored Procedure not found: " + proc.getProcName());
                    System.exit(-1);
                }

                Method run_meth = null;
                for (Method meth : proc_class.getDeclaredMethods())
                {
                    if (meth.getName().equals("run"))
                    {
                        run_meth = meth;
                        break;
                    }
                }
                if (run_meth == null)
                {
                    System.err.println("No run method in 'Stored Procedure': " + proc.getProcName());
                    System.exit(-1);
                }

                ParamType[] paramList = new ParamType[run_meth.getParameterTypes().length];
                for (ParamType customParam : proc.getParam())
                {
                    if (customParam.getOffset() != null)
                    {
                        paramList[customParam.getOffset()] = customParam;
                    }
                    else
                    {
                        System.err.println("Customizing param specified with no offset: " + customParam);
                        throw new RuntimeException("Customizing param specified with no offset: " + customParam);
                    }
                }
                myWL.paramGenerators[procIndex] = new ParamGenerator[paramList.length];
                myWL.params[procIndex] = new Object[paramList.length];

                for (int i = 0; i < run_meth.getParameterTypes().length; i++)
                {
                    myWL.paramGenerators[procIndex][i] =
                        GeneratorFactory.getParamGenerator(run_meth.getParameterTypes()[i],
                                                           paramList[i]);
                }
                procIndex++;
            }
            workloads.add(myWL);
            System.err.println("Workload " + myWL.name + " successfully built.");
            return true;
        }
        catch (Exception e)
        {e.printStackTrace();
            System.err.println("Building failed due to syntax errors in XML file.");
            return false;
        }
    }

    private boolean runLoader(Microbenchmark mb)
    {
        if (mb == null)
            return false;

        LoaderType loader = mb.getLoader();
        System.err.println("Running loader " + loader.getLoaderName());
        List<LoaderType.LoaderClass> loaderClasses = loader.getLoaderClass();

        ListIterator<LoaderType.LoaderClass> loaderClassLI = loaderClasses.listIterator();
        String pathName;
        while (loaderClassLI.hasNext())
        {
            pathName = loaderClassLI.next().getPathName();

            try
            {
                Class<?> loaderClass = Class.forName(pathName);
                Method[] methods = loaderClass.getMethods();
                //for (int i = 0; i < methods.length; i++)
                //    System.err.println(i + ": " + methods[i].getName());
                //Method run = loaderClass.getMethod("run", m_voltClient.getClass());
                Method run = methods[0];
                run.invoke(null, m_voltClient);
            }
            catch (Exception e)
            {
                System.err.println("Retrieving loader class with path name " + pathName + " failed.");
                e.printStackTrace();
                return false;
            }
        }

        System.err.println("All loading successfully completed.");
        return true;
    }

    //HOW TO HANDLE ERRORS IN XML FILE WHERE TYPE DOESN'T MATCH
    private void setParams(Workload myWL, int procIndex, int paramIndex)
    {
        ParamGenerator generator = myWL.paramGenerators[procIndex][paramIndex];
        Object randVal = null;

        randVal = generator.getNextGeneratedValue();

        myWL.params[procIndex][paramIndex] = randVal;
    }

    // Why was this not used?
    /*private File getXMLFile()
    {
        String pathName = "";
        File file = null;

        Scanner scan;
        boolean fileFound = false;
        while (!fileFound)
        {
            scan = new Scanner(System.in);
            System.err.print("Enter absolute path for XML file: ");
            pathName = scan.nextLine();
            file = new File(pathName);
            fileFound = file.exists() && pathName.toLowerCase().endsWith(".xml");
        }

        System.err.println("XML file found.");
        return file;
    }*/

    private Microbenchmark unmarshal(File xmlFile)
    {
        Microbenchmark mb = null;
        try
        {
            JAXBContext jc = JAXBContext.newInstance("org.voltdb.benchmark.workloads.xml");
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            //ADD SYNTAX VALIDATION
            mb = (Microbenchmark)unmarshaller.unmarshal(xmlFile);
        }
        catch (JAXBException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
        return mb;
    }

    private Microbenchmark unmarshal(InputStream iStream)
    {
        Microbenchmark mb = null;
        try
        {
            JAXBContext jc = JAXBContext.newInstance("org.voltdb.benchmark.workloads.xml");
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            //ADD SYNTAX VALIDATION
            mb = (Microbenchmark)unmarshaller.unmarshal(iStream);
        }
        catch (JAXBException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
        return mb;
    }

    private void testPrint()
    {
        try
        {
            VoltTable table = m_voltClient.callProcedure("SelectAll").getResults()[0];
            int numRows = table.getRowCount();
/*
            for (int i = 0; i < numRows; i++)
                System.err.println("ID " + table.fetchRow(i).get(0, table.getColumnType(0)) + " with item " + table.fetchRow(i).get(1, table.getColumnType(1)));
*/
            System.err.println(numRows + " ids total.");
        }
        catch (VoltAbortException e)
        {
            System.err.println("ERROR");
        }
        catch (ConnectException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
        catch (ProcCallException e)
        {
            System.err.println("Procedure call error: " + e.getMessage());
        }
    }
}