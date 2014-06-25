"""
Temporary placeholder to exec Jedi client remotely.
To be replaced with upcoming DEFT API functions
"""

import ast
import os.path
import re
import subprocess
from django.utils import timezone
from django.core.exceptions import ObjectDoesNotExist

import atlas.settings
from .models import ProductionTask

RSA_KEY_FILE = "%s/%s" % (os.path.dirname(os.path.abspath(atlas.settings.__file__)),
                          "jediclient-ssh/id_rsa")


def _exec_jedi_command(task_id, command, *params):
    """
    Perform JEDI command for the task.
    :param task_id: task ID
    :param command: command to perform
    :param params: additional command parameters for JEDI client
    :return: dict containing keys 'accepted', 'registered', 'jedi_message', 'jedi_status_code'
    """
    # TODO: add logging and permissions checking
    jedi_commands = ['killTask', 'finishTask', 'changeTaskPriority',
                     'reassignTaskToSite', 'reassignTaskToCloud']

    if not command in jedi_commands:
        raise ValueError("JEDI command not supported: '%s'" % (command))

    (task_id, params) = (str(task_id), [str(x) for x in params])
    action_call = "import jedi.client as jc; print jc.%s(%s)" % (command, ",".join(list([task_id])+params))

    proc = subprocess.Popen(['ssh', '-q',
                             '-i', RSA_KEY_FILE,
                             '-o', 'StrictHostKeyChecking=no',
                             '-o', 'UserKnownHostsFile=/dev/null',
                             '-o', 'LogLevel=QUIET',
                             'sbelov@aipanda015',
                             'PYTHONPATH=/mnt/atlswing/site-packages/ python -c "%s"' % (action_call),
                             '2>/dev/null'
                            ],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = proc.communicate()
    bash_lines = re.compile("^bash:")
    # removing shell warnings if any
    out = [x.rstrip() for x in out if not bash_lines.match(x)]
    out = filter(None, out)  # remove empty lines

    result = {}
    if not out:
        return result

    jedi_response = tuple(ast.literal_eval(out[0]))
    if not jedi_response:
        return result

    (accepted, registered, message) = (False, False, '')
    (status_code, return_code) = (0, 0)

    status_code = jedi_response[0]
    if status_code == 0:
        accepted = True

        if command == "changeTaskPriority":
            return_code = jedi_response[1]
        else:
            (return_code, message) = jedi_response[1]
        registered = bool(return_code)
        result['jedi_return_code'] = return_code
    else:
        message = jedi_response[1]

    result.update(accepted=accepted, registered=registered,
                  jedi_message=message, jedi_status_code=status_code)

    return result


def kill_task(task_id):
    """
    Kill task with all it jobs.
    :param task_id: ID of task to kill
    :return: dict with action status
    """
    return _exec_jedi_command(task_id, "killTask")


def finish_task(task_id):
    """
    Finish task with all it jobs.
    :param task_id: task ID
    :return: dict with action status
    """
    return _exec_jedi_command(task_id, "finishTask")


def obsolete_task(task_id):
    """
    Mark task as 'obsolete'
    :param task_id: task ID
    :return: dict with action status
    """
    # TODO: add logging and permissions checking
    try:
        task = ProductionTask.objects.get(id=task_id)
    except ObjectDoesNotExist:
        return dict(accepted=True, registered=False, message="Task %s does not exist")
    except Exception as error:
        return dict(accepted=True, registered=False, message=error)

    if task.status not in ['done', 'finished']:
        return {}

    #TODO: log action
    ProductionTask.objects.filter(id=task_id).update(status='obsolete', timestamp=timezone.now())
    return dict(accepted=True, registered=True)


def change_task_priority(task_id, priority):
    """
    Set task JEDI priority.
    :param task_id:
    :param priority: JEDI task priority
    :return: dict with action status
    """
    return _exec_jedi_command(task_id, "changeTaskPriority", priority)


def reassign_task_to_site(task_id, site):
    """
    Reassign task to specified site.
    :param task_id: task ID
    :param site: site name
    :return: dict with action status
    """
    return _exec_jedi_command(task_id, "reassignTaskToSite", site)


def reassign_task_to_cloud(task_id, cloud):
    """
    Reassign task to specified cloud
    :param task_id: task ID
    :param cloud: cloud name
    :return: dict with action status
    """
    return _exec_jedi_command(task_id, "reassignTaskToCloud", cloud)

