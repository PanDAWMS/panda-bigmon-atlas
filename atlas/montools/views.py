###########
# montools view
###########

from datetime import datetime, timedelta
import time
import json

from django.template import Context, Template, RequestContext
from django.template.loader import get_template
from django.template.response import TemplateResponse

from django.conf import settings

from django.http import HttpResponse
from django.shortcuts import render_to_response, render, redirect
from django.db.models import Count
from django.utils import timezone
from atlas.settings import STATIC_URL, FILTER_UI_ENV, defaultDatetimeFormat

from core.pandajob.models import PandaJob, Jobsactive4, Jobsdefined4, Jobswaiting4, Jobsarchived4, Jobsarchived
from core.common.models import JediTasks

statelist = [ 'defined', 'waiting', 'pending', 'assigned', 'throttled', \
             'activated', 'sent', 'starting', 'running', 'holding', \
             'transferring', 'finished', 'failed', 'cancelled', 'merging']

taskstatelist = [ 'registered', 'defined', 'assigning', 'ready', 'pending', 'scouting', 'scouted', 'running', 'prepared', 'done', 'failed', 'finished', 'aborting', 'aborted', 'finishing', 'topreprocess', 'preprocessing', 'tobroken', 'broken', 'toretry', 'toincexec', 'rerefine' ]
taskstatelist_short = [ 'reg', 'def', 'assgn', 'rdy', 'pend', 'scout', 'sctd', 'run', 'prep', 'done', 'fail', 'finish', 'abrtg', 'abrtd', 'finishg', 'toprep', 'preprc', 'tobrok', 'broken', 'retry', 'incexe', 'refine' ]
taskstatedict = []
for i in range (0, len(taskstatelist)):
    tsdict = { 'state' : taskstatelist[i], 'short' : taskstatelist_short[i] }
    taskstatedict.append(tsdict)

def dateConfigure(request_GET):
    errors_GET = {}
    try:
        ndays = int(request_GET['ndays'])
    except:
        ndays = 3
        errors_GET['ndays'] = 'Wrong or no ndays has been provided'
    
    hours = 24
    hours = 24*ndays
    startdate = timezone.now() - timedelta(hours=hours)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = timezone.now().strftime(defaultDatetimeFormat)
  
    return startdate, enddate, errors_GET
 
def testPlot(request):
    #print "===================="
    #print request.GET.get('name','')  
    from django.db.models import get_app, get_models
    for model in get_models():
         print model.__name__, [x.name for x in model._meta.fields]


    #app = get_app(app_name)
    #for model in get_models(app):
    #    print model._meta.db_table

    data = {\
       'test': 'hello world',
       'datax': [1,2,3],
       'datay': [3,4,5],
       'STATIC_URL': settings.STATIC_URL, \
    } 
    return render_to_response('plot.html', data, RequestContext(request))

def showPlot(request):
    startdate, enddate, errors_GET = dateConfigure(request.GET)
    ###request parameters
    group  = request.GET.get('group','AP_')
    status = request.GET.get('status','running')
    
    query={}
    query['modificationtime__range'] = [startdate, enddate]
    
    if group and group != 'all':
       query['workinggroup__contains'] = group

    if status:
       query['jobstatus'] = status

    querynotime = query
    del querynotime['modificationtime__range']
    res=[]
    res.extend(Jobsdefined4.objects.filter(**querynotime).values('workinggroup','currentpriority').order_by('workinggroup').annotate(Count('pandaid')))
    res.extend(Jobsactive4.objects.filter(**querynotime).values('workinggroup','currentpriority').order_by('workinggroup').annotate(Count('pandaid')))
    res.extend(Jobswaiting4.objects.filter(**querynotime).values('workinggroup','currentpriority').order_by('workinggroup').annotate(Count('pandaid')))
    res.extend(Jobsarchived4.objects.filter(**query).values('workinggroup','currentpriority').order_by('workinggroup').annotate(Count('pandaid')))
    
    ##formate res
    wgsum ={}

    for rec in res:
        wg  = str(rec['workinggroup'])
        pro = int(rec['currentpriority'])
        num = int(rec['pandaid__count'])
        #print rec['workinggroup']+"    "+str(rec['currentpriority']) +"   " +str(rec['pandaid__count'])

        if wg not in wgsum:
           wgsum[wg] = {}
           wgsum[wg]['data1']=[]

        wgsum[wg]['name'] = wg
        wgsum[wg]['data1'].append([pro,num])

    data = {\
       'group': group,
       'status': status,
       'groups': wgsum,
       'STATIC_URL': settings.STATIC_URL, \
    }
    return render_to_response('showPlot.html', data, RequestContext(request))
   
def groupSum(request):

    #if 'date_from' in requestParams:
    #    time_from_struct = time.strptime(requestParams['date_from'],'%Y-%m-%d')
    #    startdate = datetime.utcfromtimestamp(time.mktime(time_from_struct)).strftime(defaultDatetimeFormat)
    startdate = None
    hours = LAST_N_HOURS_MAX=12
    days=1
    if not startdate:
        startdate = timezone.now() - timedelta(hours=LAST_N_HOURS_MAX)
        startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = None
    if enddate == None:
        enddate = timezone.now().strftime(defaultDatetimeFormat)
    
    taskdays=3
    ## WG task summary
    tasksummary = wgTaskSummary(request, view='working group', taskdays=taskdays)
    ## WG job summary
    query = { 'modificationtime__range' : [startdate, enddate] }
    wgsummarydata = wgSummary(query)
    wgs = {}
    for rec in wgsummarydata:
        wg = rec['workinggroup']
        if wg == None: continue
        jobstatus = rec['jobstatus']
        count = rec['jobstatus__count']
        if wg not in wgs:
            wgs[wg] = {}
            wgs[wg]['name'] = wg
            wgs[wg]['count'] = 0
            wgs[wg]['states'] = {}
            wgs[wg]['statelist'] = []
            for state in statelist:
                wgs[wg]['states'][state] = {}
                wgs[wg]['states'][state]['name'] = state
                wgs[wg]['states'][state]['count'] = 0
        wgs[wg]['count'] += count
        wgs[wg]['states'][jobstatus]['count'] += count

    errthreshold = 15
    ## Convert dict to summary list
    wgkeys = wgs.keys()
    wgkeys.sort()
    wgsummary = []
    for wg in wgkeys:
        for state in statelist:
            wgs[wg]['statelist'].append(wgs[wg]['states'][state])
            if int(wgs[wg]['states']['finished']['count']) + int(wgs[wg]['states']['failed']['count']) > 0:
                wgs[wg]['pctfail'] = int(100.*float(wgs[wg]['states']['failed']['count'])/(wgs[wg]['states']['finished']['count']+wgs[wg]['states']['failed']['count']))

        wgsummary.append(wgs[wg])
    if len(wgsummary) == 0: wgsummary = None


    if request.META.get('CONTENT_TYPE', 'text/plain') == 'text/plain':
        xurl = extensibleURL(request)
        data = {
            'viewParams' : '',
            'requestParams' : '',
            'url' : request.path,
            'xurl' : xurl,
            'user' : None,
            'wgsummary' : wgsummary,
            'taskstates' : taskstatedict,
            'tasksummary' : tasksummary,
            'hours' : hours,
            'days' : days,
            'errthreshold' : errthreshold,
        }
        return render_to_response('workingGroups.html', data, RequestContext(request))
    elif request.META.get('CONTENT_TYPE', 'text/plain') == 'application/json':
        resp = []
        return  HttpResponse(json.dumps(resp), mimetype='text/html')


def wgTaskSummary(request, fieldname='workinggroup', view='production', taskdays=3):
    """ Return a dictionary summarizing the field values for the chosen most interesting fields """
    query = {}
    hours = 24*taskdays
    startdate = timezone.now() - timedelta(hours=hours)
    startdate = startdate.strftime(defaultDatetimeFormat)
    enddate = timezone.now().strftime(defaultDatetimeFormat)
    query['modificationtime__range'] = [startdate, enddate]
    if fieldname == 'workinggroup': query['workinggroup__isnull'] = False
    if view == 'production':
        query['tasktype'] = 'prod'
    elif view == 'analysis':
        query['tasktype'] = 'anal'
    summary = JediTasks.objects.filter(**query).values(fieldname,'status').annotate(Count('status')).order_by(fieldname,'status')
    totstates = {}
    tottasks = 0
    wgsum = {}
    for state in taskstatelist:
        totstates[state] = 0
    for rec in summary:
        wg = rec[fieldname]
        status = rec['status']
        count = rec['status__count']
        if status not in taskstatelist: continue
        tottasks += count
        totstates[status] += count
        if wg not in wgsum:
            wgsum[wg] = {}
            wgsum[wg]['name'] = wg
            wgsum[wg]['count'] = 0
            wgsum[wg]['states'] = {}
            wgsum[wg]['statelist'] = []
            for state in taskstatelist:
                wgsum[wg]['states'][state] = {}
                wgsum[wg]['states'][state]['name'] = state
                wgsum[wg]['states'][state]['count'] = 0
        wgsum[wg]['count'] += count
        wgsum[wg]['states'][status]['count'] += count

    ## convert to ordered lists
    suml = []
    for f in wgsum:
        itemd = {}
        itemd['field'] = f
        itemd['count'] = wgsum[f]['count']
        kys = taskstatelist
        iteml = []
        for ky in kys:
            iteml.append({ 'kname' : ky, 'kvalue' : wgsum[f]['states'][ky]['count'] })
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x:x['field'])
    return suml

def wgSummary(query):
    summary = []
    querynotime = query
    del querynotime['modificationtime__range']
    summary.extend(Jobsdefined4.objects.filter(**querynotime).values('workinggroup','jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobsactive4.objects.filter(**querynotime).values('workinggroup','jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobswaiting4.objects.filter(**querynotime).values('workinggroup','jobstatus').annotate(Count('jobstatus')))
    summary.extend(Jobsarchived4.objects.filter(**query).values('workinggroup','jobstatus').annotate(Count('jobstatus')))
    return summary

def extensibleURL(request, xurl = ''):
    """ Return a URL that is ready for p=v query extension(s) to be appended """
    if xurl == '': xurl = request.get_full_path()
    if xurl.endswith('/'): xurl = xurl[0:len(xurl)-1]
    if xurl.find('?') > 0:
        xurl += '&'
    else:
        xurl += '?'
    #if 'jobtype' in requestParams:
    #    xurl += "jobtype=%s&" % requestParams['jobtype']
    return xurl

