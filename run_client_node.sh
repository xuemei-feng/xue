#!/bin/bash

pkill -9 run_client_node
pkill -9 main_client

./project/cmake/build/run_client_node
