import json
from django.http import HttpResponseForbidden

from atlas.art.models import PackageTest, TestsInTasks
from atlas.prodtask.models import ProductionTask
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework.authtoken.models import Token
from rest_framework.authentication import TokenAuthentication, BasicAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import parser_classes
from rest_framework.parsers import JSONParser


def parse_task(task_id):

    if not PackageTest.objects.filter(task_id=task_id).exists():
        new_package_test = PackageTest()
        new_package_test.task = ProductionTask.objects.get(id=task_id)
        name = new_package_test.task.name
        new_package_test.nightly_release = '.'.join(name.split('.')[3:-5])
        new_package_test.project = name.split('.')[-5]
        new_package_test.platform = name.split('.')[-4]
        new_package_test.nightly_tag = name.split('.')[-3]
        new_package_test.sequence_tag = name.split('.')[-2]
        new_package_test.package = name.split('.')[-1].replace('/','')
        new_package_test.save()
        return new_package_test
    else:
        return PackageTest.objects.get(task_id=task_id)


def get_all_package_test():
    tasks = list(ProductionTask.objects.filter(username='artprod'))
    map(parse_task,tasks)



@api_view(['POST'])
@authentication_classes((TokenAuthentication, BasicAuthentication))
@permission_classes((IsAuthenticated,))
@parser_classes((JSONParser,))
def create_atr_task(request):
    """
    Sending task action to JEDI.
    \narguments:
       \n * username: user from whom action will be made. Required
       \n * task: task id. Required
       \n * userfullname: user full name for analysis tasks.
       \n * parameters: dict of parameters. Required

    """
    if request.user.username != 'art_api':
        return HttpResponseForbidden()
    try:
        data = json.loads(request.body)
        if not PackageTest.objects.filter(task_id=data['task_id']).exists():
            print data
            new_package_test = PackageTest()
            new_package_test.task = ProductionTask.objects.get(id=data['task_id'])
            for x in ['nightly_release','project','platform','nightly_tag','sequence_tag','package']:
                new_package_test.__dict__[x] = data[x]
            new_package_test.save()
            if 'test_names' in data:
                for test_name, index in enumerate(data['test_names']):
                    test_in_tasks = TestsInTasks()
                    test_in_tasks.test_index = index
                    test_in_tasks.name = test_name
                    test_in_tasks.package_test = new_package_test
                    test_in_tasks.save()
    except Exception,e:
        return  Response(str(e),400)
    return  Response({'status':'OK'})


def fill_test_names(file_path):
    with open(file_path) as data_file:
       data_loaded = json.load(data_file)
    for test in data_loaded:
         pack_test = PackageTest.objects.get(id=test['id'])
         for test_name,index in enumerate(test['test_names']):
             tit = TestsInTasks()
             tit.test_index = test_name
             tit.package_test = pack_test
             tit.name = index
             tit.save()