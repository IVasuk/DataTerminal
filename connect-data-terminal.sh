#!/bin/bash

echo 'Створення бази даних терміналу'
python3 dt_dbms.py -r create_tables -n local

wait
echo 'Створення публікацій терміналу'
python3 dt_dbms.py -r create_publications -n local

wait
echo 'Створення підписки до терміналу в глобальній БД'
python3 dt_dbms.py -r push_subscriptions -n local -ra [адреса глобальної БД] -rp [порт глобальної БД] -ap [адреса публікації для як її бачить глобальна БД] -pp [порт публікації для як його бачить глобальна БД]

wait
echo 'Підписка до глобальної БД'
python3 dt_dbms.py -r create_subscriptions -n local -ap [адреса глобальної БД] -pp [порт глобальної БД]

wait
echo 'Створення дефолтного елемента довідника Термінали'
python3 dt_dbms.py -r create_default_terminal -n local

