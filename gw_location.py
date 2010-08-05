#!/usr/bin/env python
# vim: set expandtab ts=4 sw=4:

import os
import re
import sys
import time
import logging
from subprocess import Popen, PIPE
from xml.dom import minidom

# Init logging
try:
    LOG_FILE = os.environ['GW_LOCATION'] + "/var/location.log"
    logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)
except:
    print >> sys.stderr, "GW_LOCATION isn't defined"
    sys.exit(1)



class LocationScheduler:

    def __init__(self, mapping_file, blacklist_file=None):

        try:
            self.GW_LOCATION = os.environ['GW_LOCATION']
        except:
            print >> sys.stderr, "GW_LOCATION isn't defined"


        if mapping_file[0] != "/":
            mapping_file = self.GW_LOCATION + "/" + mapping_file

        try:
            mapping_f = open(mapping_file, "r")
            mapping = mapping_f.read()
            mapping_f.close
        except Exception, e:
            print >> sys.stderr, "Couldn't load mapping file '%s'" % mapping_file
            sys.exit(1)
        if blacklist_file:
            if blacklist_file[0] != "/":
                blacklist_file = self.GW_LOCATION + "/" + blacklist_file
            try:
                blacklist_f = open(blacklist_file, "r")
                blacklist = blacklist_f.read()
                blacklist_f.close
            except:
                print >> sys.stderr, "Couldn't load blacklist file '%s'" % blacklist_file
                sys.exit(1)

        self.execute_data_map = self._parse_mapping(mapping)

        try:
            self.blacklist = self._parse_blacklist(blacklist)
        except UnboundLocalError:
            self.blacklist = []

        self.gwps = self.GW_LOCATION + "/bin/gwps"
        self.gwhost = self.GW_LOCATION + "/bin/gwhost"
        self.jobs = []
        self.hosts = []
        self._update_data()

    def _update_data(self):
        gwpsf = Popen([self.gwps, "-f"], stdout=PIPE, stderr=PIPE)
        stdout, stderr = gwpsf.communicate()
        self.jobs = self._parse_gwps(stdout)

        gwhostf = Popen([self.gwhost, "-f"], stdout=PIPE, stderr=PIPE)
        stdout, stderr = gwhostf.communicate()
        self.hosts = self._parse_gwhost(stdout)
        
        self._apply_blacklist()

    def _apply_blacklist(self):
        for host in self.blacklist:
            try:
                del self.hosts[host]
            except:
                pass
        
    @classmethod
    def _parse_gwps(self, gwpsout):
        """
        _parse_gwps - returns a list of job dictionaries
        """
        raw_jobs = gwpsout.split("\n\n")
        
        jobs = []

        for raw_job in raw_jobs:
            job = {}
            lines = raw_job.split("\n")
            for line in lines:
                try:
                    attribute, value = line.split("=")
                except:
                    continue
                job[attribute] = value
            if job:
                jobs.append(job)

        return jobs

    def _parse_mapping(self, mapping_xml):
        """
        _parse_mapping - 
        """

        map = {}

        mapping_xml = mapping_xml.strip()
        locationmap = minidom.parseString(mapping_xml)
        if locationmap.firstChild.nodeName == "locationmap":
            locationmap = locationmap.firstChild
            for mapping in locationmap.childNodes:
                if mapping.hasChildNodes() and mapping.nodeName == "mapping":

                    for item in mapping.childNodes:
                        if item.hasChildNodes():
                            if item.nodeName == "datasite" and item.firstChild.nodeName == "#text":
                                datasite = item.firstChild.data.strip()

                            if item.nodeName == "executionsite" and item.firstChild.nodeName == "#text":
                                executionsite = item.firstChild.data.strip()
                        
                        try:
                            map[str(datasite)] = str(executionsite)
                        except:
                            continue
        return map

    def _parse_blacklist(self, blacklist_xml):
        """
        _parse_blacklist - parses a gavia-style blacklist

        looks like:
          <blacklist>
            <site>silicon</site>
            <site>vmcgs23</site>
          </blacklist>

        """

        blacklist = []

        blacklist_xml = blacklist_xml.strip()
        blacklist_parsed = minidom.parseString(blacklist_xml)
        if blacklist_parsed.firstChild.nodeName == "blacklist":
            blacklist_parsed = blacklist_parsed.firstChild
            for site in blacklist_parsed.childNodes:
                if site.hasChildNodes() and site.nodeName == "site" and site.firstChild.nodeName == "#text":
                    blacksite = site.firstChild.data.strip()
                    blacklist.append(str(blacksite))

        return blacklist


    def _data_for_job(self, jobid):
        """
        _data_for_job - returns a data location structure

        parses a structure like: 
        <data>
            <location>
                <host>data.westcoast.ca</host>
                <size>100</size>
            </location>
            <location>
                <host>data.westcoast.ca</host>
                <size>100</size>
            </location>
        </data>
        """
        
        data_locations = {}

        job_template_file = self.GW_LOCATION + "/var/%s/job.template" % jobid
        try:
            jtfh = open(job_template_file, "r")
            job_template = jtfh.read()
            jtfh.close()
        except:
            return data_locations


        lines = job_template.split("\n")
        for line in lines:
            if line.startswith("ENVIRONMENT="):
                dataxml_match = re.search(r"<data>.*</data>", line)
                try:
                    dataxml = dataxml_match.group(0)
                except:
                    continue
                data = minidom.parseString(dataxml)
                if data.firstChild.nodeName == "data":
                    data = data.firstChild
                    for location in data.childNodes:
                        if location.hasChildNodes() and location.nodeName == "location":

                            for item in location.childNodes:
                                if item.hasChildNodes():
                                    if item.nodeName == "host" and item.firstChild.nodeName == "#text":
                                        host = item.firstChild.data.strip()

                                    if item.nodeName == "size" and item.firstChild.nodeName == "#text":
                                        size = item.firstChild.data.strip()

                            try:
                                host = str(host)
                                data_locations[host] = int(size)
                            except:
                                continue

        return data_locations


    @classmethod
    def _parse_gwhost(self, gwhostout):
        """
        _parse_gwhost - returns a list of host dictionaries
        """
        raw_hosts = gwhostout.split("\n\n")
        
        hosts = {}

        for raw_host in raw_hosts:
            host = {}
            lines = raw_host.split("\n")
            for line in lines:
                try:
                    attribute, value = line.split("=")
                except:
                    continue
                host[attribute] = value
            if host != {}:
                hosts[host["HOSTNAME"]] = host
        return hosts

    def schedule(self):
        """
        schedule -- schedule jobs

        a sample job looks like:

        JOB_ID=1
        NAME=gwt
        USER=dev01
        UID=0
        FIXED_PRIORITY=0
        DEADLINE=0:00:00
        TYPE=single
        NP=1
        JOB_STATE=pend
        EM_STATE=----
        RESTARTED=0
        CLIENT_WAITING=0
        RESCHEDULE=0
        START_TIME=16:22:43
        EXIT_TIME=--:--:--
        EXEC_TIME=0:00:00
        XFR_TIME=0:00:00
        """

        self._update_data()
        jorb = ""
        for job in self.jobs:
            jorb += str(job)
            if job.has_key("JOB_STATE") and job["JOB_STATE"] == "pend":
                job_data = self._data_for_job(job["JOB_ID"])

                if not job_data:
                    execute_site_names = self.hosts.keys()
                    execute_site = execute_site_names[0]
                    self._dumb_schedule(job)
                    continue

                data_sizes = job_data.keys()
                data_sizes.sort(key=job_data.__getitem__)
                biggest_site = data_sizes[-1]
                #print biggest_site
                #print self.execute_data_map[biggest_site]


                try:
                    execute_site_hostname = self.execute_data_map[biggest_site]
                    try:
                        execute_site = self.hosts[execute_site_hostname]
                        self._schedule_job_to(job["JOB_ID"], execute_site_hostname)
                    except KeyError:
                        self._dumb_schedule(job)
                    
                except:
                    raise
                    reason = "%s isn't an available site" % execute_site
                    self._fail_scheduling_job(job["JOB_ID"], reason)
        gw_message("SCHEDULE_END - SUCCESS -\n")
        return jorb

    def _dumb_schedule(self, job):
        """
        _dumb_schedule -- a dumb scheduling algorithm to fall back to when location-based
                          doesn't work

                          right now, all it does is pick the first resource from gwhost

        """
        #print >> sys.stderr, "Falling back to dumb scheduling"
        execute_site_names = self.hosts.keys()
        execute_site = execute_site_names[0]
        self._schedule_job_to(job["JOB_ID"], execute_site)
        
    def _schedule_job_to(self, job_id, site):
        """
        _schedule_job_to -- print GW scheduling string to schedule a job
        """
        site_data = self.hosts[site]
        queue = site_data['QUEUE_NAME[0]']
        host_id = site_data['HOST_ID']
        command = "SCHEDULE_JOB %s SUCCESS %s:%s:0\n" % (job_id, host_id, queue)
        gw_message(command)

    def _fail_scheduling_job(self, job_id, reason=""):
        """
        _fail_scheduling_job -- print GW scheduling failure string
        """
        gw_message("SCHEDULE_JOB %s FAILURE %s\n" % (job_id, reason))


def usage():
    print >> sys.stderr, "usage: %s -m mapping_file" % sys.argv[0]

GW_INIT_COMMAND = "INIT"
GW_INIT_SUCCESS = "INIT - SUCCESS -\n"
GW_SCHEDULE_COMMAND = "SCHEDULE - - - - -"
GW_FINALIZE_COMMAND = "FINALIZE"
GW_FINALIZE_SUCCESS = "FINALIZE - SUCCESS -\n"

def main():

    #if "-m" not in sys.argv:
        #usage()
        #sys.exit(1)

    args = []
    for arg in sys.argv:
        args.extend(arg.split())
 
    for i in range(0, len(args) ):
        if args[i] == "-m":
            try:
                mapping_file = args[i+1]
            except:
                usage()
                sys.exit(1)
        if args[i] == "-b":
            try:
                blacklist_file = args[i+1]
            except:
                usage()
                sys.exit(1)

    logging.info("Location scheduler started (%s)\n" % os.getpid())

    try:
        scheduler = LocationScheduler(mapping_file, blacklist_file=blacklist_file)
    except Exception, e:
        logging.warning("Blacklist file not found, trying without it\n")
        try:
            scheduler = LocationScheduler(mapping_file)
        except Exception, e:
            usage()
            sys.exit(1)


    while True:
        gw_command = raw_input()
        logging.debug("Got message: '%s'" % gw_command.replace('\n', '\\n'))
        if gw_command.startswith(GW_INIT_COMMAND):
            gw_message(GW_INIT_SUCCESS)
        elif gw_command.startswith(GW_SCHEDULE_COMMAND):
            jorbs = scheduler.schedule()
            
        elif gw_command.startswith(GW_FINALIZE_COMMAND):
            gw_message(GW_FINALIZE_SUCCESS)
            sys.exit(0)

def gw_message(command):
    """
    gw_message -- tell gridway to do something
    """
    import sys
    sys.stdout.write(command)
    sys.stdout.flush()
    logging.debug("Sent message: '%s'" % command.replace('\n', '\\n'))

if __name__ == "__main__":
    main()
