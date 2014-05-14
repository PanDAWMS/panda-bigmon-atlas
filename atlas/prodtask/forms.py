from django import forms
from django.forms import ModelForm
from django.forms import CharField
from django.forms import EmailField
from django.forms import Textarea
from django.forms import FileField
from django.forms import DecimalField
from django.forms import Form
from models import TRequest, ProductionTask, StepExecution, MCPattern, MCPriority
from django.forms.widgets import TextInput


class RequestForm(ModelForm):
    cstatus =  CharField(label='Status', required=False)

    class Meta:
        model = TRequest


class TRequestCreateCloneConfirmation(ModelForm):
    long_description = CharField(widget=Textarea, required=False)
    cc = EmailField(required=False)
    description = CharField(label='Short description', widget=Textarea, required=False)
    cstatus = CharField(widget=forms.HiddenInput, required=False)

    class Meta:
        model = TRequest
        exclude = ['reqid']



class TRequestMCCreateCloneForm(TRequestCreateCloneConfirmation):
    excellink = CharField(required=False, label="Spreadsheet Link")
    excelfile = FileField(required=False, label="Spreadsheet File")
    manager = CharField(widget=forms.HiddenInput, required=False)


    class Meta:
        model = TRequest
        exclude = ['reqid']


class TRequestDPDCreateCloneForm(TRequestCreateCloneConfirmation):
    excellink = CharField(required=False, label="DPD link")
    excelfile = FileField(required=False, label="DPD file")
    provenance = CharField(widget=forms.HiddenInput, required=False)
    cstatus = CharField(widget=forms.HiddenInput, required=False)
    request_type = CharField(widget=forms.HiddenInput, required=False)

    class Meta:
        model = TRequest
        exclude = ['reqid']


class TRequestReprocessingCreateCloneForm(TRequestCreateCloneConfirmation):
    excellink = CharField(required=False, label="First step LIST link")
    excelfile = FileField(required=False, label="First step LIST file")
    provenance = CharField(widget=forms.HiddenInput, required=False)
    cstatus = CharField(widget=forms.HiddenInput, required=False)
    request_type = CharField(widget=forms.HiddenInput, required=False)
    tag_hierarchy = CharField(help_text='tag hierarhy as python list with tuples as branches',
                              widget=Textarea, required=False)

    class Meta:
        model = TRequest
        exclude = ['reqid']

class MCPatternForm(ModelForm):

    def __init__(self, *args, **kwargs):
        steps = kwargs.pop('steps')
        super(MCPatternForm, self).__init__(*args, **kwargs)
        for step, value in steps:
            self.fields['custom_%s' % step] = CharField(label=step, required=False)
            if value:
                self.data['custom_%s' % step] = value



    def steps_dict(self):
        return_dict = {}
        for name, value in self.cleaned_data.items():
            if name.startswith('custom_'):
                return_dict.update({self.fields[name].label: value})
        return return_dict

    class Meta:
        model = MCPattern
        exclude = ['id','pattern_dict']

class MCPatternUpdateForm(MCPatternForm):

    class Meta:
        model = MCPattern
        exclude = ['id','pattern_dict','pattern_name']


class MCPriorityForm(ModelForm):

    def __init__(self, *args, **kwargs):
        steps = kwargs.pop('steps')
        super(MCPriorityForm, self).__init__(*args, **kwargs)
        for step, value in steps:
            self.fields['custom_%s' % step] = DecimalField(label=step, required=True)
            if value:
                self.data['custom_%s' % step] = value



    def steps_dict(self):
        return_dict = {}
        for name, value in self.cleaned_data.items():
            if name.startswith('custom_'):
                return_dict.update({self.fields[name].label: int(value)})
        return return_dict

    class Meta:
        model = MCPriority
        exclude = ['id','priority_dict']


class MCPriorityUpdateForm(MCPriorityForm):

    class Meta:
        model = MCPriority
        exclude = ['id','priority_dict','priority_key']

class RequestUpdateForm(Form):
    pattern_name = CharField(required=False, label="DPD link")


class StepExecutionForm(ModelForm):
    class Meta:
        model = StepExecution


class ProductionTaskForm(ModelForm):
    class Meta:
        model = ProductionTask


class ProductionTaskCreateCloneForm(ModelForm):
    class Meta:
        model = ProductionTask


class ProductionTaskUpdateForm(ModelForm):
    class Meta:
        model = ProductionTask
        widgets = {
            'id': TextInput(attrs={'readonly': 'readonly'}),
        }