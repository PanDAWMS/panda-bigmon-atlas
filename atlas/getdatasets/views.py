import json
import logging
import os

from django.core import serializers
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import render
from .forms import RequestForm

from .models import ProductionDatasetsExec
from ..settings import dq2client as settings

#Django-tables2
import django_tables2 as tables
import itertools


_logger = logging.getLogger('prodtaskwebui')

def request_data(req):
	if req.startswith('data'):
        	return request_data_dq2(req)
	else:
		return request_data_table(req)


def request_data_table(req):
	_logger.debug("Search for datasets in DB")
        req=req.replace('*','%')
        values = ProductionDatasetsExec.objects.extra(where=['name like %s'], params=[req]).exclude(status__iexact = u'deleted')
        dslist = values.values('name')
        counter = itertools.count()
        outlist=[]
        for ds in dslist:
                data_dict={}
                data_dict['name'] = ds['name']
                #data_dict['size'] = 
                data_dict['selection'] = ds['name']
                #data_dict['number'] = u'%d' % next(counter)
                outlist.append(data_dict)

        return outlist

def request_data_dq2(req):
	_logger.debug("Search for datasets in DQ2")
	outputdq2={}
	try:
		#To work with DQ2
		from dq2.clientapi.DQ2 import DQ2
		
		os.environ['RUCIO_ACCOUNT'] = settings.RUCIO_ACCOUNT
        	dq2 = DQ2(certificate=settings.PROXY_CERT)
        	
        	outputdq2 = dq2.listDatasets(dsn=req,onlyNames=True)
	except ImportError, e:
		_logger.error("No DQ2")
		raise e
	except Exception, e:
		raise e
        dslist = list(outputdq2) 
        counter = itertools.count()
	outlist=[]
        for ds in dslist:
	        data_dict={}
		data_dict['name'] = ds
		data_dict['size'] = dq2.getDatasetSize(ds)
		if  data_dict['size'] != 0:
			data_dict['selection'] = ds
			#data_dict['number'] = u'%d' % next(counter)
                	outlist.append(data_dict) 
        return outlist

class ProductionDatasetsTable(tables.Table):
        num = tables.Column(empty_values=(),orderable=False)
	name = tables.Column(orderable=False)
	selection = tables.CheckBoxColumn(attrs = { "th__input":
                                        {"onclick": "toggle(this)"}},
                                        orderable=False )

	def __init__(self, *args, **kwargs):
		super(ProductionDatasetsTable, self).__init__(*args, **kwargs)
		self.counter = itertools.count(1)

	def render_num(self):
		return '%d' % next(self.counter)

def request_data_form2(request):
        if request.method == 'POST':
                form = RequestForm(request.POST)
                pks = request.POST.getlist("selection")
		if pks:
			#print list(pks)
			return HttpResponse(json.dumps(list(pks)), content_type="application/json")
		else:
                	if form.is_valid():
                        	req = form.cleaned_data['request']
                                dslist = request_data(req)
				table=ProductionDatasetsTable(dslist)
                        	return render(request, '_request_table.html', {
                        	'active_app': 'getdatasets',
                        	'pre_form_text': 'Datasets search',
                        	'form': form,
				'table': table,
                        	'submit_text': 'Select',
                        	'submit_url': 'getdatasets:request_data_form',
                        	'parent_template': 'prodtask/_index.html',
                        	})

        else:
                form = RequestForm()
                return render(request, '_request_table.html', {
                'active_app': 'getdatasets',
                'pre_form_text': 'Datasets search',
                'form': form,
                'submit_url': 'getdatasets:request_data_form',
                'parent_template': 'prodtask/_index.html',
                })

def request_data_form(request):
        if request.method == 'POST':
                pks = request.POST.getlist("selection")
                if pks:
                        return HttpResponse(json.dumps(list(pks)), content_type="application/json")
                else:
			dslist=[]
			if 'dpat1' in request.POST:
				req = request.POST['dpat1']		
				if req:	
                                	dslist = request_data(req)
			if 'dpat2' in request.POST:
				req = request.POST['dpat2']		
				if req:	
                                	dslist = dslist+request_data(req)
			if 'dpat3' in request.POST:
				req = request.POST['dpat3']		
				if req:	
                                	dslist = dslist+request_data(req)
			reslist=[]
			for i in dslist:
				if i not in reslist:
					reslist.append(i)
                        table=ProductionDatasetsTable(reslist)
                        return render(request, '_request_table.html', {
                                'active_app': 'getdatasets',
                                'pre_form_text': 'Datasets search',
                                'table': table,
                                'submit_text': 'Select',
                                'submit_url': 'getdatasets:request_data_form',
                                'parent_template': 'prodtask/_index.html',
                                })
#	if 'dpat' in request.POST:
#		req = request.POST['dpat']		

	else:
		return render(request, '_request_table.html', {
                'active_app': 'getdatasets',
                'pre_form_text': 'Datasets search',
                'submit_url': 'getdatasets:request_data_form',
                'parent_template': 'prodtask/_index.html',
                })
	
