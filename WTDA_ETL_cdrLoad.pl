#!/usr/bin/perl
## @ (#) $Id
########################################################################
##Program Name : WTDA_ETL_cdrLoad.pl
## COPYRIGHT
## Copyright (c) 2006 Tayana Software Solutions Pvt Ltd, All rights reserved.
## No part of this computer program may be used or reproduced in any form by
## any means without any prior permission of Tayana Software Solutions Pvt Ltd
##
##Project : Wire-Free TDA (WTDA)
##
##Program Description : This is a generic module to run etl load jobs,
##It takes Cdr Category as Argument

##Originated by : Manjuvani H N on 30-01-2014
###############################################################################
#

use strict;
use POSIX qw(strftime);
use Time::HiRes qw(gettimeofday usleep);

my $moduleName  =qq~Tool Load Jobs~;
my $myProgram   =qq~WTDA_ETL_cdrLoad.pl~;
my $configFile  ="WTDA_configFile.cfg" ;

my $cdrCategory =$ARGV[0];
chomp($cdrCategory);
#Categories :IN_IPBILL IN_IPBILL6a IN_MTR IN_RECHARGE MSC SGSN VAS_CHAT VAS_PRBT VAS_ETOPUP VAS_SELFCARE VAS_WEB2SMS
$cdrCategory    =uc($cdrCategory);

my $processId =$ARGV[1];
chomp($processId);

my $logFileName = qq~WTDA_ETL_cdrLoad~.qq~_$cdrCategory~.qq~_$processId.log~;
my ($loadScriptName,$loadScriptPath,$configParam,$fileMask,$dupChk)='';
my $noOfInstance=0;
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)='';

if($cdrCategory !~ m/^[a-z\_A-Z0-9]+$/)
{
	print "No Valid Category as argument, killing the program and exiting\n";
	print "perl WTDA_ETL_cdrLoad.pl [CDR_CATEGORY] [PROCESS_ID]\n";
	exit();
}

if ($processId !~ m/^\d+$/)
{
	print "No Valid Process Id specified, killing the program and exiting\n";
	print "perl WTDA_ETL_cdrTransform.pl [CDR_CATEGORY] [PROCESS_ID]\n";
	exit();
}

#Check if process is already running
my @ret = `ps -elf | grep -v grep | grep -i "$myProgram $cdrCategory $processId "`;
if (0 < $#ret)
{
	print "Program $myProgram already running with Category $cdrCategory $processId\n";
	exit;
}
#Modules Required for this program Handling Signals
$SIG{TERM}=\&closePrg;
$SIG{HUP} =sub { die; };
$SIG{INT} =\&closePrg;
$SIG{USR1} = \&handleSignal;

my $Signal = 'Y';
($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();

use WTDA_generic;                        #   Loading generic Module
my $generic = new WTDA_generic;          #  Generic object creation

my @category    = split(/_/,$cdrCategory);
my $subCategory = @category[$#category];
my $fileMask    ="";
my ($loadPath,@cdrfileAry)='';

while(1)
{
	eval
	{
		$Signal = 'N';
		loadParam();
		main();
	};
	sleep 1;
}
sub main()
{
	while(1)
	{
		if($Signal eq 'Y')
		{
			$Signal = 'N';
			loadParam();
		}
		my $fileExt = "pid$processId*$fileMask*";
		if ($dupChk == 1)
                {
                        $fileExt= "Transnd_*_pid$processId*$fileMask*";
                }
                else
                {
                        $fileExt= "Transd_*_pid$processId*$fileMask*";
                }

		@cdrfileAry  = `cd $loadPath;ls -lc $fileExt --time-style=full-iso 2>/dev/null`;
		my $cdrArrayArySize=scalar(@cdrfileAry);
		if($cdrArrayArySize > 0)
		{ 
			$generic->writeGenericLog("$moduleName","$logFileName","Backend","1","$cdrArrayArySize files found in $loadPath with Ext $fileMask");
			my $timestamp = int (gettimeofday * 1000);
			$generic->writeGenericLog("$moduleName","$logFileName","Backend","1"," Load start $cdrArrayArySize $timestamp\n");
			`cd $loadScriptPath;sh $loadScriptName  $fileExt $processId`;
			
			my $timestamp = int (gettimeofday * 1000);
			$generic->writeGenericLog("$moduleName","$logFileName","Backend","1"," Load end $cdrArrayArySize  $timestamp\n");			
			usleep(1000000);

			$generic->writeGenericLog("$moduleName","$logFileName","Backend","1","Files with Extension $fileExt is picked for loading");
		}
		usleep(1000000);
	}
}
sub loadParam()
{
	my $loadString = "WTDA_LOAD_$subCategory";
	$loadPath = $generic->readConfig($loadString,$configFile);

	$configParam = "WTDA_FILE_MASK_".$subCategory;
	$fileMask=$generic->readConfig($configParam,$configFile);

	$configParam = "WTDA_LOAD_SCRIPT_PATH_".$subCategory;
	$loadScriptPath=$generic->readConfig($configParam,$configFile);

	$configParam = "WTDA_LOAD_SCRIPT_NAME_".$subCategory;
	$loadScriptName=$generic->readConfig($configParam,$configFile);

	$configParam = "WTDA_NO_OF_INSTANCES_".$subCategory;
	$noOfInstance=$generic->readConfig($configParam,$configFile);
		
	$configParam = "WTDA_DUPLICATE_CHECK_$subCategory";
        $dupChk =$generic->readConfig($configParam,$configFile);

	$generic->writeGenericLog("$moduleName","$logFileName","Backend","1","Fetched All file mask and script name from config file");
}

sub closePrg
{
	my $sig = shift(@_);
	$generic->writeGenericLog("$moduleName","$logFileName","Backend","1","*** Got SIG $sig ... Closing Program");
	exit();
}
sub handleSignal ()
{
	$Signal = 'Y';
	loadParam();
	$generic->writeGenericLog("$moduleName","$logFileName","Backend","1","$myProgram *** Got SIG 10 ---- Reloading config file ");
	return;
}
