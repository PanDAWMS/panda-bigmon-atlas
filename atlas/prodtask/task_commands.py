"""
Temporary placeholder to exec Jedi client remotely.
To be replaced with upcoming DEFT API functions
"""

import os.path
import subprocess
import atlas.settings

rsa_key_file = "%s/%s" %(os.path.dirname(os.path.abspath(atlas.settings.__file__)), "ssh/id_rsa")

def killTask(task_id):
    p = subprocess.Popen(["ssh", "-i", rsa_key_file, "sbelov@aipanda015",
                          'PYTHONPATH=/mnt/atlswing/site-packages/ python -c "import jedi.client as jc; print jc.killTask(%s)"' % (task_id),
                          "2>/dev/null"
                         ],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = p.communicate()
    return out

def changeTaskPriority(task_id, priority):
    p = subprocess.Popen(["ssh", "-i", rsa_key_file, "sbelov@aipanda015",
                          'PYTHONPATH=/mnt/atlswing/site-packages/ python -c "import jedi.client as jc; print jc.changeTaskPriority(%s,%s)"' % (task_id, priority),
                          "2>/dev/null"
                         ],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    out = p.communicate()
    return out
