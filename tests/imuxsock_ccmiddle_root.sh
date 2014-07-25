# note: we must be root and no other syslogd running in order to
# carry out this test
echo \[imuxsock_ccmiddle_root.sh\]: test trailing LF handling in imuxsock
echo This test must be run as root with no other active syslogd
source $srcdir/diag.sh init
source $srcdir/diag.sh startup imuxsock_ccmiddle_root.conf
# send a message with trailing LF
./syslog_inject -c
# the sleep below is needed to prevent too-early termination of rsyslogd
./msleep 100
source $srcdir/diag.sh shutdown-when-empty # shut down rsyslogd when done processing messages
source $srcdir/diag.sh wait-shutdown	# we need to wait until rsyslogd is finished!
cmp rsyslog.out.log $srcdir/resultdata/imuxsock_ccmiddle.log
if [ ! $? -eq 0 ]; then
echo "imuxsock_ccmiddle_root.sh failed"
exit 1
fi;
source $srcdir/diag.sh exit
