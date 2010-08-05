Gridway Location-Based Scheduler
================================

Created as part of NEP-02 for SAFORAH

INTRODUCTION
------------

The location based scheduler was designed with the idea that grid jobs often
have extremely large data sizes, but relatively short runtimes. As such it can
make sense to schedule to the grid site that is closest to your data. Keep in
mind that closest can refer to geography, or your network. It's up to you to
decide. 

To do this, the user provides a list of the sizes and locations of the data
required for a job, and the scheduler determines the optimal location to route
the job to based on these data.


INSTALLATION
------------

To install the scheduler, you'll need to copy 3 files to your Gridway
installation directory. This should be exported as $GW_LOCATION. As your
Gridway administrator user (usually either gwadmin or globus), do the
following:

    $ echo $GW_LOCATION
    /usr/local/gridway
    $ cp gw_location.py $GW_LOCATION/bin/
    $ cp map.xml blacklist.xml $GW_LOCATION/etc/

Now verify that your installation worked:
 
    $ $GW_LOCATION/bin/gw_location.py
    usage: /usr/local/gridway/bin/gw_location.py -m mapping_file

Great! It worked!

Next, you'll need to modify your gwd.conf file, and set your dispatch manager
to the location scheduler, to do that, comment out the line in your config
beginning with DM_SCHED, and replace it with the following line:

    DM_SCHED  = location:gw_location.py:-m etc/map.xml -b etc/blacklist.xml

Now restart Gridway, and confirm that the scheduler has started correctly:

    $ grep "Scheduler location loaded" $GW_LOCATION/var/gwd.log
    Wed Aug  4 17:07:04 2010 [DM][I]: 	Scheduler location loaded 
    (exec: gw_location.py, arg: -m etc/map.xml -b etc/blacklist.xml).

If it hasn't loaded properly, you can check the log file in 
`$GW_LOCATION/var/location.log`


PATCHING GRIDGATEWAY
--------------------

Location based Scheduling requires a patch to GridGateWay, the Web Services
front end to GridWay. This patch forwards location information provided in the
job to GridWay. You can apply this patch in-place to an existing GridWay
installation. This patch is included as "ggw_location.patch" in this source
directory.

Run the following to patch your installation:

    $ cd $GLOBUS_LOCATION
    $ patch -p0 < /path/to/ggw_location.patch
    patching file /usr/local/globus-4.0.8/lib/perl/Globus/GRAM/JobManager/gw.pm


CONFIGURATION
-------------

This scheduler has two config files, map.xml and blacklist.xml:

map.xml -- provides a list of mappings of data sites, to execution sites. These
mappings are used by the scheduler to determine where the closest cluster is to
this data. For example, if you have data in data.eastcoast.ca, and that data is
closest to cluster.halifax.edu, you would create the following mapping:

    <locationmap>
      <mapping>
        <datasite>data.eastcoast.ca</datasite>
        <executionsite>cluster.halifax.ca</executionsite>
      </mapping>
    </locationmap>

Add one of these mappings for each data site.


blacklist.xml -- This is a list of sites that you would like the scheduler to
never schedule to. This is optional. It is in the following format:

    <blacklist>
      <!-- change these example blacklisted sites -->
      <site>cluster.badsite.ca</site>
      <site>cluster.slowsite.ca</site>
    </blacklist>

To use these files, use the -m and -b options when invoking gw_location.py.


JOB SUBMISSION
--------------

When submitting a job to a Gridway installation using this scheduler, you must
provide a list of the data locations and sizes in your job description. This is
done in the following format:

    <data>
      <location>
        <host>data.eastcoast.ca</host>
        <size>10</size>
      </location>
      <location>
        <host>data.westcoast.ca</host>
        <size>100</size>
      </location>
    </data>

The units for data are arbitrary, but must be consistent for each location
element for the scheduler to make sensible decisions. For example, always use
megabytes as your unit, or always use gigabytes.

This data location information should be placed in an extensions element in
your job description. Here is a trivial, but complete Globus job as an example:

    <job>
      <executable>/bin/uname</executable>
      <directory>${GLOBUS_USER_HOME}</directory>
      <argument>-a</argument>
      <stdout>${GLOBUS_USER_HOME}/stdout.${GLOBUS_JOB_ID}.txt</stdout>
      <stderr>${GLOBUS_USER_HOME}/stderr.${GLOBUS_JOB_ID}.txt</stderr>
      <extensions>
        <data>
          <location>
            <host>data.eastcoast.ca</host>
            <size>10</size>
          </location>
          <location>
            <host>data.westcoast.ca</host>
            <size>100</size>
          </location>
        </data>
      </extensions>
    </job>

TODO
----

* Clean up code in gw_location.py
* Fall back to builtin scheduler when location data is not available
