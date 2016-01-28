package com.datastax.support;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by abhinavchawade on 1/17/16.
 */
public class ExecutorLogDownloader implements Runnable
{
    private static final Logger log = LoggerFactory.getLogger(ExecutorLogDownloader.class);
    private static final int PAGE_SIZE = 1048576; //Fetch 1 MBytes worth of data in one HTTP request

    private String workerHost, appId;
    private int executorNumber;
    private Path targetDirectoryPath;

    public ExecutorLogDownloader(String workerHost, String appId, int executorNumber, String targetDirectory)
    {
        this.workerHost = workerHost;
        this.executorNumber = executorNumber;
        this.appId = appId;
        targetDirectoryPath = Paths.get(targetDirectory + File.separator + executorNumber);
    }

    public void run()
    {
        /**
         * http://192.168.56.102:8081/logPage?appId=app-20160106184227-0006&executorId=0&logType=stdout
         * http://192.168.56.102:8081/logPage?appId=app-20160108202849-0010&executorId=0&logType=stdout&offset=0&byteLength=102400
         */

        String stdoutUri = workerHost + "/logPage?appId=" + appId +
                "&executorId=" + executorNumber + "&logType=stdout";
        String stderrUri = workerHost + "/logPage?appId=" + appId +
                "&executorId=" + executorNumber + "&logType=stderr";
        Document stdoutPage, stderrPage;
        int stdoutLength, stderrLength;

        try
        {
            stdoutPage = Jsoup.connect(stdoutUri).get();
            stderrPage = Jsoup.connect(stderrUri).get();
        }
        catch (IOException ioe)
        {
            log.error("Failed to download worker: {} executor: {}. Reason:{}", workerHost, executorNumber, ioe.getLocalizedMessage());
            return;
        }

        try
        {
            Files.createDirectories(targetDirectoryPath);
        }
        catch (IOException ioe)
        {
            log.error("Failed to create output directory. Reason:{}", ioe.getLocalizedMessage());
            return;
        }

        //get length and then loop
        String stdoutSizeString = stdoutPage.select("span:contains(Bytes)").get(0).text();
        String stderrSizeString = stderrPage.select("span:contains(Bytes)").get(0).text();
        stdoutLength = Integer.valueOf(stdoutSizeString.substring(stdoutSizeString.lastIndexOf(" of ") + 4));
        stderrLength = Integer.valueOf(stderrSizeString.substring(stderrSizeString.lastIndexOf(" of ") + 4));
        log.debug("Download lengths for worker:{} executor:{} stdout:{} bytes stderr:{} bytes",
                workerHost,executorNumber,stdoutLength,stderrLength);

        BufferedWriter stdoutFile, stderrFile;
        try
        {
            Charset outputCharset = Charset.forName("UTF-8");
            stdoutFile = Files.newBufferedWriter(Paths.get(targetDirectoryPath.toString()
                    + File.separator + "stdout"), outputCharset);
            stderrFile = Files.newBufferedWriter(Paths.get(targetDirectoryPath.toString()
                    + File.separator + "stderr"), outputCharset);
        }
        catch (IOException ioe)
        {
            log.error("Unable to create executor log files worker:{} executor:{}. Reason:{}",
                    workerHost,executorNumber,ioe.getLocalizedMessage());
            return;
        }

        log.debug("Worker:{} executor:{} stdout and stderr created. Downloading",workerHost,executorNumber);

        int stdoutOffset = 0, stderrOffset = 0;
        while (stdoutOffset < stdoutLength || stderrOffset < stderrLength)
        {
            if (stdoutOffset < stdoutLength)
                try
                {
                    String stdoutCurrentText = Jsoup.connect(stdoutUri + "&offset=" + stdoutOffset
                            + "&byteLength=" + PAGE_SIZE).get().select("pre").text();
                    stdoutFile.write(stdoutCurrentText, stdoutOffset, stdoutCurrentText.length());
                    stdoutOffset += PAGE_SIZE;
                }
                catch (IOException ioe)
                {
                    log.error("Failed to download stdout from worker:{} executor:{}. Reason:{}",
                            workerHost, executorNumber, ioe.getLocalizedMessage());
                    return;
                }

            if (stderrOffset < stderrLength)
                try
                {
                    String stderrCurrentText = Jsoup.connect(stderrUri + "&offset=" + stderrOffset
                            + "&byteLength=" + PAGE_SIZE).get().select("pre").text();
                    stderrFile.write(stderrCurrentText, stderrOffset, stderrCurrentText.length());
                    stderrOffset += PAGE_SIZE;
                }
                catch (IOException ioe)
                {
                    log.error("Failed to download stderr from worker:{} executor:{}. Reason:{}",
                            workerHost, executorNumber, ioe.getLocalizedMessage());
                    return;
                }
        }

        if(stdoutFile != null)
            try
            {
                stdoutFile.close();
            }
            catch(IOException ioe)
            {
                log.error("Could not close stdout for worker:{} executor:{}. Reason:{}",
                        workerHost,executorNumber,ioe.getLocalizedMessage());
                return;
            }

        if(stderrFile != null)
            try
            {
                stderrFile.close();
            }
            catch(IOException ioe)
            {
                log.error("Could not close stderr for worker:{} executor:{}. Reason:{}",
                        workerHost,executorNumber,ioe.getLocalizedMessage());
                return;
            }
        log.debug("Downloaded executor {} stdout and stderr from {}", executorNumber, workerHost);
    }
}
