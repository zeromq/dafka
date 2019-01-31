#!/bin/sh

# Use clang-format tool to verify this codebase conforms to our style standards

FAILED=0
IFS=";"
FILES="../include/dafka.h;../include/dafka_library.h;../include/dafka_proto.h;../include/dafka_publisher.h;../include/dafka_store.h;../include/dafka_subscriber.h;../src/dafka_classes.h;../src/dafka_private_selftest.c;../src/dafka_proto.c;../src/dafka_publisher.c;../src/dafka_selftest.c;../src/dafka_store.c;../src/dafka_stored.c;../src/dafka_subscriber.c;../src/dakfa_console_producer.c"
IDS=$(echo -en "\n\b")
[ -n "$MAKE" ] || MAKE=make
for FILE in $FILES
do
    clang-format -style=file -output-replacements-xml "$FILE" | grep "<replacement " >/dev/null && \
    {
      echo "$FILE is not correctly formatted" >&2
      FAILED=1
    }
done
if [ "$FAILED" -eq "1" ] ; then
    if [ "$1" = "--CI" ] ; then
        echo "Style mismatches were found by clang-format; detailing below:" >&2
        ${MAKE} clang-format-diff
        if test x"${CI_REQUIRE_GOOD_CLANG_FORMAT}" = xtrue ; then
            echo "FAILED : Style checks have failed and CI_REQUIRE_GOOD_CLANG_FORMAT==true" >&2
            exit 1
        fi
        echo "WARNING : Style checks have failed, but the result is not-fatal because CI_REQUIRE_GOOD_CLANG_FORMAT!=true" >&2
        exit 0
    fi
    exit 1
fi
