#!/bin/bash

echo 'Creating dataterminal1'
python3 dt-dbms.py -d dataterminal1 -t dataterminal1

wait
echo 'Creating dataterminal2'
python3 dt-dbms.py -d dataterminal2 -t dataterminal2

wait
echo 'Creating dataterminal'
python3 dt-dbms.py -a 192.168.204.253 -n global -t dataterminal

wait
echo 'Suscribing dataterminal to dataterminal1'
python3 dt-dbms.py -a 192.168.204.253 -n global -t dataterminal1 -ap 192.168.204.18 -r subscriber -dp dataterminal1

wait
echo 'Suscribing dataterminal to dataterminal2'
python3 dt-dbms.py -a 192.168.204.253 -n global -t dataterminal2 -ap 192.168.204.18 -r subscriber -dp dataterminal2

wait
echo 'Suscribing dataterminal1 to dataterminal'
python3 dt-dbms.py -d dataterminal1 -t dataterminal1 -ap 192.168.204.253 -r subscriber -dp dataterminal

wait
echo 'Suscribing dataterminal2 to dataterminal'
python3 dt-dbms.py -d dataterminal2 -t dataterminal2 -ap 192.168.204.253 -r subscriber -dp dataterminal
