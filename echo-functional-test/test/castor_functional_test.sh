#!/bin/bash
#
#Tests CASTOR remotely via SRM and xroot
#
# mviljoen 26-Nov-2013
#

#additional flags to pass to the tests, e.g. "-v" for verbose output
#more info
#LCG_ARGS="-v"
#XRD_ARGS="-v"
#GFAL_ARGS="-v"

#less info
XRD_ARGS="--nopbar"

#Set the test parameters for each CASTOR instance to be tested.  An instance per line.
#Arguments are passed to the do_tests function and are as follows:
# $1 CASTOR instance name
# $2 VO name to test against
# $3 SRM endpoint
# $4 xrootd test type (r = read; rw = read/write; d = disable)
# $5 xrootd manager
# $6 HSM location
# ${7-*} Space token(s) to use - at least one
#
ARGS_ATLAS="atlas atlas srm-atlas.gridpp.rl.ac.uk rw catlasdlf.ads.rl.ac.uk /castor/ads.rl.ac.uk/test/func ATLASSCRATCHDISK ATLASDATADISK ATLASGROUPDISK"
ARGS_CMS="cms cms srm-cms.gridpp.rl.ac.uk r ccmsdlf.ads.rl.ac.uk /castor/ads.rl.ac.uk/test/func CMSDISK CMS_DEFAULT"
ARGS_ALICE="gen dteam srm-alice.gridpp.rl.ac.uk rw cgendlf.ads.rl.ac.uk /castor/ads.rl.ac.uk/test/func srm2_d0t1"
ARGS_GEN="gen dteam srm-dteam.gridpp.rl.ac.uk rw cgenlsf.ads.rl.ac.uk /castor/ads.rl.ac.uk/test/func srm2_d0t1"
ARGS_LHCB="lhcb lhcb srm-lhcb.gridpp.rl.ac.uk rw clhcbstager.ads.rl.ac.uk /castor/ads.rl.ac.uk/test/func LHCb-Disk LHCb_USER"
ARGS_FAC="fac dteam fdssrm01.fds.rl.ac.uk rw fdscdlf05.fds.rl.ac.uk /castor/facilities/test/func DIAMOND CEDA etjasmin cedaRetrieve FacD0T1 diamondRecall"
#ARGS_PREPROD_DISK="preprod dteam preprod-srm-haproxy.gridpp.rl.ac.uk rw cpredlf.ads.rl.ac.uk /castor/preprod.ral/preprodDisk/test/d1t0 PreprodDiskPool"
ARGS_PREPROD="preprod dteam lcgsrm08.gridpp.rl.ac.uk rw cpredlf.ads.rl.ac.uk /castor/preprod.ral/preprodDisk/test/d1t0 PreprodDiskPool PreprodDiskPool"
#ARGS_PREPROD="preprod dteam lcgsrm08.gridpp.rl.ac.uk rw cpredlf.ads.rl.ac.uk /castor/preprod.ral/preprodDisk/test/d1t0 PreprodDiskPool PreprodTape"
ARGS_VCERT="vcert dteam lcgsrm21.gridpp.rl.ac.uk rw lcgcdlf21.gridpp.rl.ac.uk /castor/vcert.ral/test/vCertD1T0 vCertD1T0"

#Other settings
LCGCP="time -p `which lcg-cp`"
LCGDEL="time -p `which lcg-del`"
GFALCP="`which gfal-copy`"
GFALRM="`which gfal-rm`"
XRDCP="time -p `which xrdcp`"
FILENAME_SRM=$(echo $(date +%Y%m%d-%H%M%S)_SRM)
FILENAME_XR=$(echo $(date +%Y%m%d-%H%M%S)_XR)

YELLOW="\033[1;33m"
RED="\033[0;31m"
BLUE="\033[0;34m"
COLOUR_RESET="\033[0m"

#LOCAL_SRC_FILE=/tmp/mviljoen/bigfiletest
LOCAL_SRC_FILE=/etc/hosts
LOCAL_RCL_FILE=$(pwd)/recall

if [ $# -lt 1 ]
then
        echo "Usage : $0 (atlas|cms|alice|gen|lhcb|allprod|fac|preprod|preprod_all|vcert|alltest)"
        echo
        exit
fi

. /etc/init.d/functions

# Use step(), try(), and next() to perform a series of commands and print
# [  OK  ] or [FAILED] at the end. The step as a whole fails if any individual
# command fails.
#
# Example:
#     step "Remounting / and /boot as read-write:"
#     try mount -o remount,rw /
#     try mount -o remount,rw /boot
#     next
step() {
    echo -n "$@"

    STEP_OK=0
    [[ -w /tmp ]] && echo $STEP_OK > /tmp/step.$$
}

try() {
    # Check for `-b' argument to run command in the background.
    local BG=

    [[ $1 == -b ]] && { BG=1; shift; }
    [[ $1 == -- ]] && {       shift; }

    # Run the command.
    if [[ -z $BG ]]; then
        "$@"
    else
        "$@" &
    fi

    # Check if command failed and update $STEP_OK if so.
    local EXIT_CODE=$?

    if [[ $EXIT_CODE -ne 0 ]]; then
        STEP_OK=$EXIT_CODE
        [[ -w /tmp ]] && echo $STEP_OK > /tmp/step.$$

        if [[ -n $LOG_STEPS ]]; then
            local FILE=$(readlink -m "${BASH_SOURCE[1]}")
            local LINE=${BASH_LINENO[0]}

            echo "$FILE: line $LINE: Command \`$*' failed with exit code $EXIT_CODE." >> "$LOG_STEPS"
        fi
    fi

    return $EXIT_CODE
}

next() {
    [[ -f /tmp/step.$$ ]] && { STEP_OK=$(< /tmp/step.$$); rm -f /tmp/step.$$; }
    [[ $STEP_OK -eq 0 ]]  && echo_success || echo_failure
    echo

    return $STEP_OK
}

function exe_cmd {
   echo -e $BLUE"\nEXECUTING: \"$1\""$COLOUR_RESET
   $1
}

function do_tests {
   #sanity check
   if [ $# -lt 5 ]
   then
      echo "Wrong number of vars sent to: $0 while testing instance: '$1' !"
      exit 1
   fi

   INS=$1
   VO=$2
   SRM=$3
   XR_TEST=$4
   XR_REDIR=$5
   HSM=$6
   #knock off first 6 variables:
   shift 6

   for ST in "$@"
   do
      echo ===================================================================
      echo   Testing: $INS with spacetoken: $ST
      echo ===================================================================
      step "SRM copy file IN..."
#      try exe_cmd "$LCGCP $LCG_ARGS --vo $VO --defaultsetype srmv2 --nobdii -S $ST file:$LOCAL_SRC_FILE srm://$SRM:8443/srm/managerv2?SFN=$HSM/$INS/$ST/$FILENAME_SRM"
      try exe_cmd "$GFALCP $GFAL_ARGS -S $ST $LOCAL_SRC_FILE srm://$SRM:8443$HSM/$INS/$ST/$FILENAME_SRM"
      next
      step "SRM copy file OUT..."
      try exe_cmd "$GFALCP $GFAL_ARGS -S $ST srm://$SRM:8443$HSM/$INS/$ST/$FILENAME_SRM $LOCAL_RCL_FILE"
      next
      step "SRM file delete..."
      try exe_cmd "$GFALRM $GFAL_ARGS srm://$SRM:8443$HSM/$INS/$ST/$FILENAME_SRM"
      next

      #local cleanup
      rm -f $LOCAL_RCL_FILE

      if [[ $XR_TEST == "rw" ]]
      then
              step "xroot copy file IN..."
              try exe_cmd "$XRDCP $XRD_ARGS $LOCAL_SRC_FILE root://$XR_REDIR/$HSM/$INS/$ST/$FILENAME_XR"
              next
              step "xroot copy file OUT..."
              try exe_cmd "$XRDCP $XRD_ARGS root://$XR_REDIR/$HSM/$INS/$ST/$FILENAME_XR $LOCAL_RCL_FILE"
              next
              step "xroot test file delete (via SRM)..."
              try exe_cmd "$GFALRM srm://$SRM:8443$HSM/$INS/$ST/$FILENAME_XR"
              next
      elif [[ $XR_TEST == "r" ]]
      then
              #Read-only xrootd test.  We can't copy a file in, so a permanent test file needs to be there to be pulled out.
              step "xroot copy file OUT..."
              try exe_cmd "$XRDCP $XRD_ARGS root://$XR_REDIR/$HSM/$INS/$ST/permanent_test_file $LOCAL_RCL_FILE"
              next
      fi

      #local cleanup
      rm -f $LOCAL_RCL_FILE
   done
}

#Parse the arguments to determine which instance(s) to test

case "$1" in

fac)
    do_tests $ARGS_FAC
    ;;
vcert)
    do_tests $ARGS_VCERT
    ;;
preprod)
    do_tests $ARGS_PREPROD
    ;;
preprod_all)
    do_tests $ARGS_PREPROD
    ;;
atlas)
    do_tests $ARGS_ATLAS
    ;;
cms)
    do_tests $ARGS_CMS
    ;;
alice)
    do_tests $ARGS_ALICE
    ;;
gen)
    do_tests $ARGS_GEN
    ;;
lhcb)
    do_tests $ARGS_LHCB
   ;;
allprod)
    echo  "Testing all Tier 1 production instances"
    do_tests $ARGS_ATLAS
    do_tests $ARGS_CMS
    do_tests $ARGS_ALICE
    do_tests $ARGS_GEN
    do_tests $ARGS_LHCB
   ;;
alltest)
    echo  "Testing all test instances"
    do_tests $ARGS_PREPROD
    do_tests $ARGS_VCERT
   ;;
*) echo "Not recognised instance: $1"
   ;;
esac

