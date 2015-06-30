#! /bin/bash
#
# build_win.sh
# Copyright (C) 2015 oliveagle <oliveagle@gmail.com>
#
# Distributed under terms of the MIT license.
#


hash=$(git rev-parse --short HEAD)

go build . && tar -czf dist/logstash-forwarder-$hash.tgz ./logstash-forwarder.exe && echo "done"

