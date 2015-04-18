from django import forms
from django.forms import ModelForm, ModelChoiceField, MultiValueField, NullBooleanField, BooleanField
from django.forms import CharField
from django.forms import EmailField
from django.forms import Textarea
from django.forms import FileField
from django.forms import DecimalField
from django.forms import Form
import json
from models import TRequest, ProductionTask, StepExecution, MCPattern, MCPriority, TProject
from django.forms.widgets import TextInput
from django.forms import widgets

class RequestForm(ModelForm):
    cstatus =  CharField(label='Status', required=False)

    class Meta:
        model = TRequest


def energy_to_str(energy):
    if (int(energy)==5023)or(int(energy)==5020):
        return '5TeV'
    if int(energy)>1000:
        gev = str(int(energy))
        print gev[-3]
        if gev[-3]!='0':
            return gev[0:-3]+'p'+gev[-3:].rstrip('0')+'TeV'
        else:
            return gev[0:-3]+gev[-3:].rstrip('0')+'TeV'


    else:
        return str(energy)+'GeV'


class TRequestCreateCloneConfirmation(ModelForm):
    long_description = CharField(widget=Textarea, required=False)
    cc = CharField(required=False)
    description = CharField(label='Short description', widget=Textarea, required=True)
    cstatus = CharField(widget=forms.HiddenInput, required=False)
    project = ModelChoiceField(queryset=TProject.objects.all(),required=True)
    provenance = CharField(required=True)
    phys_group = CharField(required=True, widget=forms.Select(choices=TRequest.PHYS_GROUPS))
    campaign = CharField(required=True)
    need_approve = NullBooleanField(required=False,initial=True)
    need_split = NullBooleanField(widget=forms.HiddenInput, required=False,initial=False)
    split_divider = DecimalField(widget=forms.HiddenInput, required=False, initial=-1)

    class Meta:
        model = TRequest
        exclude = ['reqid','is_error','jira_reference','info_fields','is_fast']

    def clean(self):
        cleaned_data = super(TRequestCreateCloneConfirmation, self).clean()
        if type(self) == TRequestCreateCloneConfirmation:
            project = cleaned_data.get('project')
            energy = cleaned_data.get('energy_gev')
            if project:
                if 'eV' in str(project):
                    if (energy_to_str(energy) not in str(project)) and (energy_to_str(energy).replace('2p76TeV','2TeV') not in  str(project)):
                        msg = "Energy doesn't correspond project"
                        self._errors['project'] = self.error_class([msg])
                        self._errors['energy_gev'] = self.error_class([msg])
                        del cleaned_data['project']
                        del cleaned_data['energy_gev']

        return cleaned_data


class TRequestMCCreateCloneForm(TRequestCreateCloneConfirmation):

    excellink = CharField(required=False, label="Spreadsheet Link")
    #excelfile = FileField(required=False, label="Spreadsheet File")
    manager = CharField(widget=forms.HiddenInput, required=False)
    project = ModelChoiceField(queryset=TProject.objects.all(),required=False)
    phys_group = CharField(required=False, widget=forms.Select(choices=TRequest.PHYS_GROUPS))
    campaign = CharField(required=False)
    provenance = CharField(widget=forms.HiddenInput, required=False)
    description = CharField(label='Short description', widget=Textarea, required=False)
    need_approve = NullBooleanField(widget=forms.HiddenInput,required=False,initial=True)
    need_split = NullBooleanField(widget=forms.HiddenInput,required=False,initial=False)

    class Meta:
        model = TRequest
        exclude = ['reqid','is_error','jira_reference','info_fields', 'is_fast']



class TRequestDPDCreateCloneForm(TRequestCreateCloneConfirmation):
    excellink = CharField(required=False, label="DPD link")
    excelfile = FileField(required=False, label="DPD file")
    provenance = CharField(widget=forms.HiddenInput, required=False)
    cstatus = CharField(widget=forms.HiddenInput, required=False)
    request_type = CharField(widget=forms.HiddenInput, required=False)
    project = ModelChoiceField(queryset=TProject.objects.all(),required=False)
    hidden_json_slices = CharField(widget=forms.HiddenInput, required=False, label="Will be hidden")
    phys_group = CharField(required=False, widget=forms.Select(choices=TRequest.PHYS_GROUPS))
    campaign = CharField(required=False)
    description = CharField(label='Short description', widget=Textarea, required=False)
    need_approve = NullBooleanField(widget=forms.HiddenInput,required=False,initial=True)
    need_split = NullBooleanField(widget=forms.HiddenInput,required=False,initial=False)

    class Meta:
        model = TRequest
        exclude = ['reqid','is_error','jira_reference','info_fields' ,'is_fast']


class TRequestHLTCreateCloneForm(TRequestCreateCloneConfirmation):
    excellink = CharField(required=False, label="HLT LIST link")
    excelfile = FileField(required=False, label="HLT LIST file")
    provenance = CharField(widget=forms.HiddenInput, required=False)
    cstatus = CharField(widget=forms.HiddenInput, required=False)
    request_type = CharField(widget=forms.HiddenInput, required=False)
    project = ModelChoiceField(queryset=TProject.objects.all(),required=False)
    phys_group = CharField(required=False, widget=forms.Select(choices=TRequest.PHYS_GROUPS), initial='THLT')
    campaign = CharField(required=False)
    hidden_json_slices = CharField(widget=forms.HiddenInput, required=False, label="Will be hidden")
    description = CharField(label='Short description', widget=Textarea, required=False)
    need_approve = NullBooleanField(widget=forms.HiddenInput,required=False,initial=True)
    need_split = NullBooleanField(widget=forms.HiddenInput,required=False,initial=False)

    class Meta:
        model = TRequest
        exclude = ['reqid','is_error','jira_reference','info_fields','is_fast']

class PatternTextInput(widgets.MultiWidget):
    def __init__(self, attrs={'0':None,'1':None}):

        _widgets = (
            widgets.TextInput(attrs=attrs['0'] ),
            widgets.TextInput(attrs=attrs['1']),
            widgets.NumberInput(attrs=attrs['2']),
        )
        super(PatternTextInput, self).__init__(_widgets, attrs)

    def decompress(self, value):
        if value:
            return [value[0],value[1],value[2]]
        return None

    def format_output(self, rendered_widgets):
        return ''.join(rendered_widgets)

    def value_from_datadict(self, data, files, name):
        datelist = [
            widget.value_from_datadict(data, files, name + '_%s' % i)
            for i, widget in enumerate(self.widgets)]
        try:
            D = [datelist[0], datelist[1], datelist[2]]
        except ValueError:
            return ''
        else:
            return D

class DoubleCharField(MultiValueField):
    def __init__(self, *args, **kwargs):


        fields = (
            CharField(),
            CharField(),
            DecimalField()
        )
        super(DoubleCharField, self).__init__(fields=fields, *args, **kwargs)

    def compress(self, data_list):
        return json.dumps([str(x) for x in data_list])


class TRequestReprocessingCreateCloneForm(TRequestCreateCloneConfirmation):
    excellink = CharField(required=False, label="First step LIST link")
    excelfile = FileField(required=False, label="First step LIST file")
    provenance = CharField(widget=forms.HiddenInput, required=False)
    cstatus = CharField(widget=forms.HiddenInput, required=False)
    request_type = CharField(widget=forms.HiddenInput, required=False)
    project = ModelChoiceField(queryset=TProject.objects.all(),required=False)
    phys_group = CharField(required=False, widget=forms.Select(choices=TRequest.PHYS_GROUPS), initial='REPR')
    campaign = CharField(required=False)
    hidden_json_slices = CharField(widget=forms.HiddenInput, required=False, label="Will be hidden")
    description = CharField(label='Short description', widget=Textarea, required=False)
    need_approve = NullBooleanField(widget=forms.HiddenInput,required=False,initial=True)

    class Meta:
        model = TRequest
        exclude = ['reqid','is_error','jira_reference','info_fields','is_fast']






class MCPatternForm(ModelForm):

    def __init__(self, *args, **kwargs):
        steps = kwargs.pop('steps')
        super(MCPatternForm, self).__init__(*args, **kwargs)
        for step, value in steps:
            #self.fields['custom_%s' % step] = CharField(label=step, required=False)
            self.fields['custom_%s' % step] = DoubleCharField(label=step,
                                                              required=False,
                                                              widget=PatternTextInput(attrs={'0':{'placeholder':'ami tag', 'value':value[0]},
                                                                                             '1':{'placeholder':'project mode', 'value':value[1]},
                                                                                             '2':{'placeholder':'nEventsPerJob', 'value':value[2]}}))
            # if value:
            #     self.data['custom_%s' % step] = ['a','n',1]



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

class RequestUpdateForm(ModelForm):
    class Meta:
        model = TRequest
        widgets = {
            'reqid': TextInput(attrs={'readonly': 'readonly'}),
            'info_fields': TextInput(attrs={'readonly': 'readonly'}),
        }


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