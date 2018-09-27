#!/bin/bash
impala-shell $@
mail -s "Run Complete" dyoon@umich.edu < /dev/null
