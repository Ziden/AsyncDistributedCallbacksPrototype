#!C:\Users\gabri\PycharmProjects\AsyncJobs\venv\Scripts\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'pyrobuf==0.9.3','console_scripts','pyrobuf'
__requires__ = 'pyrobuf==0.9.3'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('pyrobuf==0.9.3', 'console_scripts', 'pyrobuf')()
    )
