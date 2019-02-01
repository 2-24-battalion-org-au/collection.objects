#!/bin/sh

.  venv/bin/activate

python xthings.py rpull -l . -y --no-debug

git add things/.
d=`date +"D%Y%m%d-T%H%M"`
git commit -m "things from remote as of $d"






