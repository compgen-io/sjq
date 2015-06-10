#!/bin/sh

if [ "$1" == "daemon" ]; then
        echo "Starting SJQ server as a background daemon..."
        shift
        ($0 server $@ <&- &>/dev/null &) &
        exit
fi

JAVA_OPTS="${JAVA_OPTS} -Dio.compgen.support.pid=$$"

JAVABIN=`which java`
if [ "${JAVA_HOME}" != "" ]; then
    JAVABIN="$JAVA_HOME/bin/java"
fi
exec "${JAVABIN}" ${JAVA_OPTS} -jar $0 "$@"
exit 1