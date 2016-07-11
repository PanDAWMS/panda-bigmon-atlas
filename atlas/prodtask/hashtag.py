import datetime
from django.db.models import Q
import json
import logging

from django.http import HttpResponse, HttpResponseRedirect

from django.views.decorators.csrf import csrf_protect
from time import sleep, time
from copy import deepcopy
import pytz
from rest_framework.decorators import api_view
from atlas.prodtask.helper import form_request_log
from atlas.prodtask.models import HashTag, HashTagToRequest
from .models import StepExecution, InputRequestList, TRequest, OpenEndedRequest
from rest_framework.response import Response
from django.shortcuts import render
_logger = logging.getLogger('prodtaskwebui')


def form_hashtag_string(reqid):
    hashtags_string = ''
    request_hashtags = HashTagToRequest.objects.filter(request=reqid)
    hashtags = []
    for request_hashtag in request_hashtags:
        if request_hashtag.hashtag.hashtag not in hashtags:
            hashtags.append(request_hashtag.hashtag.hashtag)
    if hashtags:
        hashtags_string = ' '.join(['#'+x for x in hashtags])
        hashtag_path = ','.join(hashtags)
    return hashtags_string,hashtag_path


@api_view(['GET'])
def request_hashtags(request, hashtags):
    hashtags_to_process = hashtags.split(',')
    result={}
    requests = []
    first_requests = True
    try:
        for hashtag in hashtags_to_process:

            if HashTag.objects.filter(hashtag__iexact=hashtag).exists():
                current_request = []
                requests_hashtags = HashTagToRequest.objects.filter(hashtag=HashTag.objects.filter(hashtag__iexact=hashtag)[0])
                for request_hashtags in requests_hashtags:

                    current_request.append(int(request_hashtags.request_id))
                if first_requests:
                    requests =   current_request
                    first_requests = False
                else:
                    new_requests = [x for x in requests if x in current_request]
                    requests = new_requests
        result.update({'requests':requests})
    except Exception,e:
        print str(e)
    return Response({"load": result})

def request_hashtags_main(request):
    if request.method == 'GET':
        return render(request, 'prodtask/_hashtag_requests.html', {
                'active_app': 'prodtask',
                'pre_form_text': 'Hashtags to request',
                'submit_url': 'prodtask:request_progress_main',
                'parent_template': 'prodtask/_index.html',
            })

@csrf_protect
def add_request_hashtag(request, reqid):
    if request.method == 'POST':
        results = {'success':False}
        try:
            data = request.body
            input_dict = json.loads(data)
            hashtag = input_dict['hashtag']
            hashtag = hashtag.replace('#','')
            _logger.debug(form_request_log(reqid,request,'Add hashtag: %s' % str(hashtag)))
            existed_hashtags = list(HashTag.objects.filter(hashtag__iexact=hashtag))
            existed_hashtag = None
            if existed_hashtags:
                existed_hashtag = existed_hashtags[0]
            else:
                existed_hashtag = HashTag()
                existed_hashtag.hashtag = hashtag
                existed_hashtag.type = 'UD'
                existed_hashtag.save()
            if not HashTagToRequest.objects.filter(hashtag=existed_hashtag,request=reqid).exists():
                request_hashtag = HashTagToRequest()
                request_hashtag.hashtag = existed_hashtag
                request_hashtag.request = TRequest.objects.get(reqid=reqid)
                request_hashtag.save()
            hashtag_html,hashtag_href = form_hashtag_string(reqid)
            results = {'success':True,'data':{'html':hashtag_html,'href':hashtag_href}}
        except Exception,e:
            pass
        return HttpResponse(json.dumps(results), content_type='application/json')


def get_include_file(file_name):
    include_files  = []
    with open(file_name,'r') as input_file:
        for line in input_file:
            if line:
                if 'include(' in line:
                    include_files.append(line[line.find('(')+2:line.find(')')-1])
    return include_files


def get_hashtag_from_file(file_name):
    keywords = []
    keywords_str = ''
    carry = False
    with open(file_name,'r') as input_file:
        for line in input_file:
            if line:
                if carry:
                    if ']' in line:
                        keywords_str +=  line[:line.find(']')]
                        break
                    else:
                        keywords_str +=  line
                if ('keywords' in line) and ('[' in line):
                    if ']' in line:
                        keywords_str = line[line.find('[')+1:line.find(']')]
                        break
                    else:
                        carry = True
                        keywords_str = line[line.find('[')+1:]



    tokens =  keywords_str.split('"')
    for token in tokens:
        if token:
            if token[0] not in [' ',',','\n','\t','\r']:
                keywords.append(token)
    return keywords
