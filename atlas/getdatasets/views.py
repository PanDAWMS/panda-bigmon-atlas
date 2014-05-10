import json

from django.core import serializers
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import render
from .forms import RequestForm

from .models import ProductionDatasetsExec
#To work with DQ2
from dq2.clientapi.DQ2 import DQ2
#Django-tables2
import django_tables2 as tables
import itertools

def request_data_table(request,req):
	print 'Request DB'
        #req=req.replace('*','.*?')
        req=req.replace('*','%')
        values = ProductionDatasetsExec.objects.extra(where=['name like %s'], params=[req])
        #values = ProductionDatasetsExec.objects.filter(name__iregex=req)
	#print values.query
        #values_list = values.values_list('name')
        values_dict = values.values('name')
        #return HttpResponse(json.dumps(list(values_list)), content_type="application/json")
        return values_dict

def request_data_dq2(request,req):
	print 'Request dq2'
        dq2 = DQ2()
        output = dq2.listDatasets(dsn=req,onlyNames=True)
        #return HttpResponse(json.dumps(list(output)), content_type="application/json")
        counter = itertools.count()
        dslist = list(output) 
	outlist=[]
        for ds in dslist:
	        dict={}
		dict['name'] = ds
		dict['size'] = dq2.getDatasetSize(ds)
		dict['selection'] = 'selected'
		dict['number'] = u'%d' % next(counter)
                outlist.append(dict) 
        return outlist

def request_data(request):
	if request.method == 'POST':
		form = RequestForm(request.POST)
		if form.is_valid():
                	req = form.cleaned_data['request'] 
			if req.startswith('data'):                    
				return request_data_dq2(request,req)	
			else:
				return request_data_table(request,req)
	else:
		form = RequestForm()
		return render(request, '_request.html', {
       		'active_app': 'getdatasets',
       		'pre_form_text': 'Request datasets',
       		'form': form,
		'submit_url': 'getdatasets:request_data',
       		'parent_template': '_index.html',
		})



def request_data_form_to_table(request):
        if request.method == 'POST':
                form = RequestForm(request.POST)
                if form.is_valid():
                        req = form.cleaned_data['request']
                        if req.startswith('data'):
                                dslist = request_data_dq2(request,req)
                        else:
                                dslist = request_data_table(request,req)
                        #print dslist
                        return render(request, '_request_table.html', {
	                'active_app': 'getdatasets',
               		'pre_form_text': 'Request datasets',
               		'form': form,
			'listlen':len(dslist),
               		'inputList': dslist,
               		'submit_url': 'getdatasets:request_data_form_to_table',
               		'parent_template': '_index.html',
               		})
 
        else:
                form = RequestForm()
                return render(request, '_request_table.html', {
                'active_app': 'getdatasets',
                'pre_form_text': 'Request datasets',
                'form': form,
                'submit_url': 'getdatasets:request_data_form_to_table',
                'parent_template': '_index.html',
                })






class ProductionDatasetsTable(tables.Table):
        num = tables.Column(empty_values=())
	name = tables.Column()
	selection = tables.CheckBoxColumn(attrs = { "th__input":
#	selection = tables.CheckBoxColumn(accessor="pk", attrs = { "th__input": 
                                        {"onclick": "toggle(this)"}},
                                        orderable=False )

	def __init__(self, *args, **kwargs):
		super(ProductionDatasetsTable, self).__init__(*args, **kwargs)
		self.counter = itertools.count(1)

	def render_num(self):
		return '%d' % next(self.counter)



def request_data_form_to_table2(request):
        if request.method == 'POST':
                form = RequestForm(request.POST)
                if form.is_valid():
                        req = form.cleaned_data['request']
                        if req.startswith('data'):
                                dslist = request_data_dq2(request,req)
                        else:
                                dslist = request_data_table(request,req)
                        #print dslist
			table=ProductionDatasetsTable(dslist)
                        pks = request.POST.getlist("selection")
			for ds in dslist:
				for i in pks:
					if ds['number'] == i:
						print ds['name']
                        #for row in table.rows:
			#	print row[2]
			#	if row[2] == 'checked':
			#		print row[1] 
                        return render(request, '_request_table2.html', {
                        'active_app': 'getdatasets',
                        'pre_form_text': 'Request datasets',
                        'form': form,
			'table': table,
                        'submit_text': 'Select',
                        'submit_url': 'getdatasets:request_data_form_to_table2',
                        'parent_template': '_index.html',
                        })

        else:
                form = RequestForm()
                return render(request, '_request_table2.html', {
                'active_app': 'getdatasets',
                'pre_form_text': 'Request datasets',
                'form': form,
                'submit_url': 'getdatasets:request_data_form_to_table2',
                'parent_template': '_index.html',
                })




