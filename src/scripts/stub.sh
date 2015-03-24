#!/bin/sh

if [ "$1" == "daemon" ]; then
    if [ "$2" == "server" ]; then
        echo "Starting server as a background daemon..."
        shift
        ($0 $@ <&- &>/dev/null &) &
        exit
    fi
fi

JAVA_OPTS="${JAVA_OPTS} -Dio.compgen.support.pid=$$"

JAVABIN=`which java`
if [ "${JAVA_HOME}" != "" ]; then
    JAVABIN="$JAVA_HOME/bin/java"
fi
exec "${JAVABIN}" ${JAVA_OPTS} -jar $0 "$@"
exit 1