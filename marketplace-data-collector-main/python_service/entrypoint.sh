#!/bin/bash

python main.py

while true; do
  # Текущее время
  CURRENT_MIN=$(date +%M)
  CURRENT_MIN=$((10#$CURRENT_MIN))
  CURRENT_SEC=$(date +%S)
  CURRENT_SEC=$((10#$CURRENT_SEC))
  TARGET_MIN=10

  # Если текущая минута меньше 10, ждем до 10-й минуты
  if [ $CURRENT_MIN -lt $TARGET_MIN ]; then
    SLEEP_TIME=$(( (TARGET_MIN - CURRENT_MIN) * 60 - CURRENT_SEC ))
  else
    # Если уже прошли 55 минут, ждем до следующего часа
    SLEEP_TIME=$(( (60 - CURRENT_MIN + TARGET_MIN) * 60 - CURRENT_SEC ))
  fi

  echo "Ожидание $SLEEP_TIME секунд до запуска..."
  sleep $SLEEP_TIME

  # Запуск Python-скрипта
  python main.py
done
