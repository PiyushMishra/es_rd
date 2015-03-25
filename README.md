#                        Web crawler in scala.
     
     It is a web crawler written in scala and akka download all the mails   
     contained in a apache maven group.
     
###                     How to run web-crawler
 1- Clone the repo.
 
 2- Decend to directory and do sbt run.

###                    How to package it using assembly.
Decend to the directory and do sbt assembly. It will make a executable web-crawler.jar in target folder.

###                    Configuration

It keeps below configuration in src/main/resources/application.conf

###### folder fo storing crwaler metadata

crawlStorageFolder=/opt/data/crawl/root

######  number of crawlers
numberOfCrawlers=1

######  year for which mail needs to be downloaded
year=2014

###### mail storage folder
mailsDownlaodFolder=/opt/mails/




     
     
