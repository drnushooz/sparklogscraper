package com.datastax.support;

import org.apache.commons.cli.*;
import org.apache.log4j.PropertyConfigurator;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by abhinavchawade on 1/15/16.
 */
public class SparkLogScraper
{
    private static final Logger log = LoggerFactory.getLogger(SparkLogScraper.class);

    public static void main(String[] args)
    {
        if (args.length < 2)
        {
            System.out.println("Syntax: sparklogscraper -m <master url> -a <app id> [-d <download directory> -t <download threads>] -z <output archive name>");
            System.exit(-1);
        }

        try
        {
            Path logConfiguration = Paths.get("./conf/log4j.properties");
            PropertyConfigurator.configure(new FileInputStream(logConfiguration.toFile()));
        }
        catch(IOException ioe)
        {
            System.out.print("Could not initialize log4j. Reason:"+ioe.getLocalizedMessage());
        }

        Options options = new Options();
        options.addOption("m", "master", true, "Spark master uri");
        options.addOption("a", "appid", true, "Application ID");
        options.addOption("d", "downloaddir", true, "Download directory");
        options.addOption("t", "threads", true, "Download threads");
        options.addOption("z", "archivename", true, "Output archive name");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmdLine = null;
        String outputArchiveName = null;
        try
        {
            cmdLine = parser.parse(options, args);
        }
        catch (ParseException pe)
        {
            log.error("Parsing failed. Reason:{}", pe.getLocalizedMessage());
            System.exit(-2);
        }

        String masterLocation = null, appId = null;
        Path targetDirectoryPath = null;
        ExecutorService downloaderPool;
        int downloaderThreadCount = 1;
        if (cmdLine != null)
        {
            masterLocation = cmdLine.getOptionValue("m");
            if(!masterLocation.endsWith("/"))
                masterLocation += "/";
            appId = cmdLine.getOptionValue("a");
            if (cmdLine.hasOption("d"))
                targetDirectoryPath = Paths.get(cmdLine.getOptionValue("d") + File.separator + appId).toAbsolutePath();
            else
                targetDirectoryPath = Paths.get("." + File.separator + appId).toAbsolutePath();
            if (cmdLine.hasOption("t"))
                downloaderThreadCount = Integer.valueOf(cmdLine.getOptionValue("t"));
            if(cmdLine.hasOption("z"))
                outputArchiveName = cmdLine.getOptionValue("z");
        }
        else
        {
            log.error("Command line cannot be null");
            System.exit(-4);
        }

        if(!Files.exists(targetDirectoryPath))
            try
            {
                Files.createDirectory(targetDirectoryPath);
            }
            catch(IOException ioe)
            {
                log.error("Unable to create output directory at {}. Reason:{}",
                        targetDirectoryPath.toString(),ioe.getMessage());
                System.exit(-5);
            }

        log.info("Getting information for app {} from {}", appId, masterLocation);
        downloaderPool = Executors.newFixedThreadPool(downloaderThreadCount);
        Document appDoc;
        Elements executorList = new Elements();
        Elements tableList = null;
        try
        {
            //http://192.168.56.102:7080/app?appId=app-20160106184344-0008
            appDoc = Jsoup.connect(masterLocation + "app?appId=" + appId).get();
            tableList = appDoc.select("table");

            //There are 2 tables in Spark 1.4+ UI. Executor summary and removed executors
            for(Element executorTables : tableList)
                executorList.addAll(executorTables.select("tbody").select("tr"));
        }
        catch (IOException ioe)
        {
            ioe.printStackTrace();
            System.exit(-3);
        }

        if (executorList != null && !executorList.isEmpty())
        {
            log.info("Total executors to get logs from {}",executorList.size());
            for (Element executorRow : executorList)
            {
                int executorId = Integer.valueOf(executorRow.select("td:first-of-type").text());
                String workerUri = executorRow.select("a:first-of-type").attr("href");
                downloaderPool.execute(new ExecutorLogDownloader(workerUri, appId, executorId, targetDirectoryPath.toString()));
            }
        }

        downloaderPool.shutdown();
        try
        {
            downloaderPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        }
        catch(InterruptedException ie)
        {
            log.error("Downloader thread pool interrupted. Reason:{}",ie.getLocalizedMessage());
            downloaderPool.shutdownNow();
        }

        if(outputArchiveName != null)
        {
            log.info("Generating output archive {}",outputArchiveName);
            ZipUtil.pack(targetDirectoryPath.getParent().toFile(),new File(outputArchiveName));
        }
        log.info("Logs downloaded to {}. All done!",targetDirectoryPath.toAbsolutePath().toString());
        System.exit(0);
    }
}