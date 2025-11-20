#!/bin/bash

docker compose logs -t python_service >> python_service_logs.txt

(docker compose down python_service) &
STOP_PID=$!

(docker compose build python_service) &
BUILD_PID=$!

wait $STOP_PID
wait $BUILD_PID

docker compose up -d python_service

docker compose logs -f python_service
