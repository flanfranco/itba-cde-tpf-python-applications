import sys
import os

# Necessary to pytest can use/reach dags folder
wd = os.getcwd()
sys.path[0] = wd + "/dags"
