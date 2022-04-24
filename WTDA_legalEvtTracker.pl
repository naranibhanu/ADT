#!/usr/bin/perl
# @ (#) $Id
#######################################################################
# Program Name : WTDA_legalEvtTracker.pl
# COPYRIGHT
# Copyright (c) 2006 Tayana Software Solutions Pvt Ltd, All rights reserved.
# No part of this computer program may be used or reproduced in any form by
# any means without any prior permission of Tayana Software Solutions Pvt Ltd
#
#Project : Wire-Free TDA (WTDA)
#
#Program Description : This is module accepts subscriber details which needs to
#be tracked and generates an excel with all req fields.
#Originated by : Banu Rajadurai on 05-03-2015
#Modified By   : Farooque D. on 12-01-2017 (To call query script only once instead of for loop-for each event) and added closePrg() and handleSignal() functions
##############################################################################

use strict;
use DB_File;
use POSIX;
use POSIX qw(strftime);
use DBI qw(:sql_types);
use Time::HiRes qw(gettimeofday usleep);
use Time::Local;
use File::Slurp ;



my $moduleName=qq~legalEvtTracker~;
my $myProgram =qq~WTDA_legalEvtTracker.pl~;

my $logFileName = qq~WTDA_legalEvtTracker.log~;

my @ret = `ps -elf | grep -v grep | grep -i "perl $myProgram"`;
if (0 < $#ret)
{
        print "Program $myProgram already running \n";
        exit;
}

use WTDA_generic;                        #   Loading generic Module
my $generic = new WTDA_generic;           #  Generic object creation

use WTDA_perlDatabase;
my ($conDb,$conDbVertica)='';
$SIG{TERM} = \&closePrg;
$SIG{HUP} = sub { die; };
$SIG{INT} = \&closePrg;
$SIG{USR1} = \&handleSignal;
my $cdrDir = '';
my $Signal = 'Y';

while(1)
{
        eval
        {
		    $conDb = new  WTDA_perlDatabase;
    		$conDb->dbCon();
            $Signal = 'N';
            loadParam();
            main();
        };
        if(defined($conDb))
        {
            $generic->writeGenericLog("$moduleName","$logFileName","Backend","3","Problem occured in main(),disconnecting");
            $conDb->dbDiscon();
        }
        sleep 1;
}

sub main()
{
    $generic->writeGenericLog("$moduleName","$logFileName","Backend","1","Started");
    while(1)
    {
        if($Signal eq 'Y')
        {
            $Signal = 'N';
            loadParam();
        }

        my $fetRecordsFrmTrackQ = qq~select USER_ID,EVENT_REQUESTED,REQ_ID,CUST_NO,START_DATE,END_DATE,DATE(REQ_START_DATE) from WTDA_LEGAL_EVENT_TRACKER where STATUS=0 order by REQ_START_DATE~;
        my $fetRecordsFrmTrackQRes = $conDb->dbSel($fetRecordsFrmTrackQ);
        unless ($fetRecordsFrmTrackQRes)
		{
			undef $fetRecordsFrmTrackQRes ;
			$conDb->dbDiscon();
			die;
		}
		while (my($usrId,$event_requested,$req_id,$cust_no,$start_date,$end_date,$req_start_date)= $fetRecordsFrmTrackQRes->fetchrow_array())
		{
# Update status to 1 as picked 
            my $updateStatus = qq~UPDATE WTDA_LEGAL_EVENT_TRACKER set STATUS=1 where REQ_ID=$req_id~;
            my $updateStatusPrep = $conDb->dbPrep($updateStatus);
            my $updateStatusExe = $conDb->dbExe($updateStatusPrep);
            undef $updateStatusPrep;
            undef $updateStatusExe;
#			my @noOfEvents = split(/,/,&getCosValue($event_requested,''));
            my $eventsString = &getCosValue($event_requested,'');
            my @eventsStringArray = split(',',$eventsString);
            my $eventsStringNew = '';
            for(my $i=0;$i<=$#eventsStringArray;$i++)
            {
                if ($eventsStringArray[$i] == 1)
                {
                    $eventsStringNew .= '0,';
                }
                elsif($eventsStringArray[$i] == 2)
                {
                    $eventsStringNew .= '1,';
                }
                elsif($eventsStringArray[$i] == 4)
                {
                    $eventsStringNew .= '6,';
                }
                elsif($eventsStringArray[$i] == 8)
                {
                    $eventsStringNew .= '7,';
                }
                elsif($eventsStringArray[$i] == 16)
                {
                    $eventsStringNew .= '100,';
                }
            }
            $eventsStringNew =~ s/,$//;

#            my $cdrDir = $generic->readConfig("WTDA_TRACKER_PATH","WTDA_configFile.cfg");
            my $final_Name = "$cdrDir/eventTrack_$req_id"."_$cust_no"."_$req_start_date".".csv";
            append_file("$final_Name","SUBSCRIBER MSISDN,CALLING NUMBER,CALLED NUMBER,CALL DATETIME,CALL DURATION,FIRST CELL NAME,LAST CELL NAME,CALL TYPE,IMSI,IMEI\n");
            $generic->writeGenericLog("$moduleName","$logFileName","Backend","1","For USER_ID= $usrId , calling script: /usr/bin/perl /opt/wtda/etc/scripts/WTDA_legalEventTrackerQuery.pl $eventsStringNew $req_id $cust_no $start_date $end_date $req_start_date &");

            `/usr/bin/perl /opt/wtda/etc/scripts/WTDA_legalEventTrackerQuery.pl $eventsStringNew $req_id $cust_no $start_date $end_date $req_start_date &`;

            $generic->writeGenericLog("$moduleName","$logFileName","Backend","1","Returned from Query Script");
            my $executionStatus = $?;
            if($executionStatus ne "0")
            {
                $generic->writeGenericLog("$moduleName","$logFileName","","3","Error in running Query script for REQ_ID=$req_id. Error Status:$executionStatus.");
            }
            $generic->writeGenericLog("$moduleName","$logFileName","Backend","1","LET Request with REQ_ID = $req_id processed sucessfully. Report filename:$final_Name");
        }
        undef $fetRecordsFrmTrackQRes ;
        sleep(30);
    }
}

sub getCosValue
{

	my($priviledgeCategary1,$fetchedValue1) = @_;
	my $cosVal = 0;
	my $i = 0;
	my $variable = "";
	while ($cosVal < $priviledgeCategary1 || $cosVal == $priviledgeCategary1 && $priviledgeCategary1 > 0)
	{
		$cosVal = 2 ** $i;
		$i++;
	}
	if( $cosVal == 1 || $cosVal < 1)
	{	
		return $fetchedValue1;
	}
	else
	{
		$i -=2;
		$cosVal = 2 ** $i;
		if( $fetchedValue1 eq ""){$fetchedValue1 = $cosVal;}
		else
		{
			$fetchedValue1 = "$fetchedValue1".","."$cosVal";
		}
		$priviledgeCategary1 = $priviledgeCategary1 - $cosVal;
		$variable = &getCosValue($priviledgeCategary1,$fetchedValue1);
    }
    return $variable;
}

sub loadParam()
{
    $cdrDir = $generic->readConfig("WTDA_TRACKER_PATH","WTDA_configFile.cfg");
    if ($cdrDir eq '')
    {
        print "Configuration value for WTDA_TRACKER_PATH is NULL in WTDA_configFile.cfg Please check. Exiting process. \n ";
        exit;
    }
    $generic->writeGenericLog("$moduleName","$logFileName","Backend","1","Fetched configurations. cdrDir = $cdrDir");
}

sub handleSignal ()
{
    $Signal = 'Y';
    $generic->writeGenericLog("$moduleName","$logFileName","Backend","1","*** Got SIG 10... Reloding from config...");
    loadParam();
    return;
}

sub closePrg
{
    my $sig = shift(@_);
    $generic->writeGenericLog("$moduleName","$logFileName","Backend","1","*** Got SIG $sig ... Closing Program.");
    $conDb->dbDiscon();
    exit();
}

