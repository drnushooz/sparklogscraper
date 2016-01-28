# sparklogscraper

Downloading spark logs on per application basis throughout the cluster can be a tedious task. This tool simplifies this task by scraping logs in a multi-threaded fashion from the master. The application takes spark master ui url and application name as input and download logs at specified location. The tool can also produced a compressed zip file for easy transport. The project uses Jsoup library to download and parse HTML and save stdout and stderr at specific locations

## Syntax
sparklogscraper -m <master url> -a <app id> [-d <download directory> -t <download threads>] -z <output archive name>

Example: bin/logscraper -m http://127.0.0.1:7080 -a app-20160126223417-0037 -t 4 -d ~/Desktop/sparkscrapertest/

## To build the project
1) git clone https://github.com/drnushooz/sparklogscraper sparklogscraper

2) cd sparklogscraper

3) mvn clean package

4) bin/logscraper

I would like to thank Jsoup creation team for such a fantastic HTML parsing library. Jsoup can be found at http://jsoup.org/
