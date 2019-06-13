#!/bin/bash


pushd ..
./mvnw verify -B -V -DskipITs
popd
