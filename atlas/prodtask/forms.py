import copy
from django import forms
from django.contrib.admin.widgets import AdminSplitDateTime
from django.db.models import Q
from django.forms import ModelForm, ModelChoiceField, MultiValueField, NullBooleanField, BooleanField, DateField, \
    DateTimeInput, DateTimeField
from django.forms import CharField
from django.forms import EmailField
from django.forms import Textarea
from django.forms import FileField
from django.forms import DecimalField
from django.forms import Form
import json
from django.forms.extras.widgets import SelectDateWidget
import re
from atlas.prodtask.models import JediWorkQueue, ParentToChildRequest
from models import TRequest, ProductionTask, StepExecution, MCPattern, MCPriority, TProject, TrainProduction, \
    RetryErrors, RetryAction, InputRequestList
from django.forms.widgets import TextInput, SplitDateTimeWidget
from django.forms import widgets

class RequestForm(ModelForm):
    cstatus =  CharField(label='Status', required=False)

    class Meta:
        model = TRequest
        fields = '__all__'

def energy_to_str(energy):
    if (int(energy)==5023)or(int(energy)==5020):
        return '5TeV'
    if int(energy) == 8160:
        return 'pPb8TeV'
    if int(energy)>1000:
        gev = str(int(energy))
        print gev[-3]
        if gev[-3]!='0':
            return gev[0:-3]+'p'+gev[-3:].rstrip('0')+'TeV'
        else:
            return gev[0:-3]+gev[-3:].rstrip('0')+'TeV'


    else:
        return str(energy)+'GeV'


def form_input_list_for_preview(file_dict):
    input_lists = []
    for slices in file_dict:
        slice = slices['input_dict']
        tags = []
        for step in slices.get('step_exec_dict'):
            tags.append(step.get('tag'))
        input_lists.append(copy.deepcopy(slice))
        input_lists[-1].update({'tags':','.join(tags)})
    return input_lists

class TRequestCreateCloneConfirmation(ModelForm):
    long_description = CharField(widget=Textarea(attrs={'cols':100,'style': 'height:300px'}), required=False)
    cc = CharField(required=False)
    description = CharField(label='Short description', widget=Textarea, required=True)
    cstatus = CharField(widget=forms.HiddenInput, required=False)
    project = ModelChoiceField(queryset=TProject.objects.filter(Q(project__startswith='mc')&Q(project__contains='_')|~Q(project__startswith='mc')),required=True)
    provenance = CharField(required=True)
    phys_group = CharField(required=True, widget=forms.Select(choices=TRequest.PHYS_GROUPS))
    campaign = CharField(required=True)
    need_approve = NullBooleanField(required=False,initial=True)
    need_split = NullBooleanField(widget=forms.HiddenInput, required=False,initial=False)
    split_divider = DecimalField(widget=forms.HiddenInput, required=False, initial=-1)
    train = ModelChoiceField(queryset=TrainProduction.objects.filter(Q(status='mc_pattern')),required=False)

    class Meta:
        model = TRequest
        exclude = ['reqid','is_error','jira_reference','info_fields','is_fast']

    def clean(self):
        cleaned_data = super(TRequestCreateCloneConfirmation, self).clean()
        if type(self) == TRequestCreateCloneConfirmation:
            project = cleaned_data.get('project')
            project_name = ''
            if project:
                project_name = project.project
            energy = cleaned_data.get('energy_gev')
            campaign = cleaned_data.get('campaign')

            if project:
                if 'eV' in str(project):
                    if (energy_to_str(energy) not in str(project)) and (energy_to_str(energy).replace('2p76TeV','2TeV') not in  str(project)):
                        msg = "Energy doesn't correspond project"
                        self._errors['project'] = self.error_class([msg])
                        self._errors['energy_gev'] = self.error_class([msg])
                        del cleaned_data['project']
                        del cleaned_data['energy_gev']
                if (cleaned_data.get('request_type') == 'MC')and(cleaned_data.get('project')):
                    if 'data' in project_name:
                        msg = "project can't be data for MC request"
                        self._errors['project'] = self.error_class([msg])
                        del cleaned_data['project']
                    elif ('valid' not in project_name) and (campaign.lower()!=project_name.split('_')[0]):
                        msg = "Campaign doesn't correspond project"
                        self._errors['project'] = self.error_class([msg])
                        self._errors['campaign'] = self.error_class([msg])
                        del cleaned_data['project']
                        del cleaned_data['campaign']


        return cleaned_data


class TRequestMCCreateCloneForm(TRequestCreateCloneConfirmation):

    excellink = CharField(required=False, label="Spreadsheet Link")
    #excelfile = FileField(required=False, label="Spreadsheet File")
    manager = CharField(widget=forms.HiddenInput, required=False)
    request_type = CharField(initial='MC', required=True)
    project = ModelChoiceField(queryset=TProject.objects.filter(Q(project__startswith='mc')&Q(project__contains='_')|~Q(project__startswith='mc')),required=False)
    phys_group = CharField(required=False, widget=forms.Select(choices=TRequest.PHYS_GROUPS))
    campaign = CharField( required=False)
    provenance = CharField(widget=forms.HiddenInput, required=False)
    description = CharField(label='Short description', widget=Textarea, required=False)
    need_approve = NullBooleanField(widget=forms.HiddenInput,required=False,initial=True)
    need_split = NullBooleanField(widget=forms.HiddenInput,required=False,initial=False)
    train = ModelChoiceField(queryset=TrainProduction.objects.filter(Q(status='mc_default_pattern')),required=False)

    class Meta:
        model = TRequest
        exclude = ['reqid','is_error','jira_reference','info_fields', 'is_fast']



class TRequestDPDCreateCloneForm(TRequestCreateCloneConfirmation):
    excellink = CharField(required=False, label="DPD link")
    excelfile = FileField(required=False, label="DPD file")
    provenance = CharField(widget=forms.HiddenInput, required=False)
    cstatus = CharField(widget=forms.HiddenInput, required=False)
    request_type = CharField(widget=forms.HiddenInput, required=False)
    project = ModelChoiceField(queryset=TProject.objects.filter(Q(project__startswith='mc')&Q(project__contains='_')|~Q(project__startswith='mc')),required=False)
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
    project = ModelChoiceField(queryset=TProject.objects.filter(Q(project__startswith='mc')&Q(project__contains='_')|~Q(project__startswith='mc')),required=False)
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
    project = ModelChoiceField(queryset=TProject.objects.filter(Q(project__startswith='mc')&Q(project__contains='_')|~Q(project__startswith='mc')),required=False)
    phys_group = CharField(required=False, widget=forms.Select(choices=TRequest.PHYS_GROUPS), initial='REPR')
    campaign = CharField(required=False)
    hidden_json_slices = CharField(widget=forms.HiddenInput, required=False, label="Will be hidden")
    description = CharField(label='Short description', widget=Textarea, required=False)
    need_approve = NullBooleanField(widget=forms.HiddenInput,required=False,initial=True)

    class Meta:
        model = TRequest
        exclude = ['reqid','is_error','jira_reference','info_fields','is_fast']

class TRequestEventIndexCreateCloneForm(TRequestCreateCloneConfirmation):
    excellink = CharField(required=False, label="First step LIST link",widget=forms.HiddenInput)
    excelfile = FileField(required=False, label="First step LIST file",widget=forms.HiddenInput)
    provenance = CharField(widget=forms.HiddenInput, required=False)
    cstatus = CharField(widget=forms.HiddenInput, required=False)
    request_type = CharField(widget=forms.HiddenInput, required=False)
    project = ModelChoiceField(queryset=TProject.objects.filter(Q(project__startswith='mc')&Q(project__contains='_')|~Q(project__startswith='mc')),required=False)
    phys_group = CharField(required=False, widget=forms.Select(choices=TRequest.PHYS_GROUPS), initial='SOFT')
    campaign = CharField(required=False)
    hidden_json_slices = CharField(widget=forms.HiddenInput, required=False, label="Will be hidden")
    description = CharField(label='Short description', widget=Textarea, required=False)
    need_approve = NullBooleanField(widget=forms.HiddenInput,required=False,initial=True)

    class Meta:
        model = TRequest
        exclude = ['reqid','is_error','jira_reference','info_fields','is_fast']


class RetryErrorsForm(ModelForm):
    id = DecimalField(widget=forms.HiddenInput,required=False)
    error_source = CharField(widget=Textarea, required=True)
    error_code = DecimalField()
    active = CharField(required=True, widget=forms.Select(choices=[(x,x) for x in ['Y','N']]))
    error_diag = CharField(widget=Textarea, required=False, help_text="Error diag follows the python regular \n"
                                                                      "expression syntax (<a href=https://docs.python.org/2/library/re.html>https://docs.python.org/2/library/re.html</a>).")
    parameters = CharField(widget=Textarea, required=False)
    retry_action = ModelChoiceField(queryset=RetryAction.objects.all(),required=True)
    architecture = CharField(widget=Textarea, required=False)
    release = CharField(required=False)
    work_queue =  ModelChoiceField(queryset=JediWorkQueue.objects.all(),required=False)
    description = CharField(widget=Textarea, required=False)
    expiration_date = DateTimeField(widget = TextInput(attrs=
                                {
                                    'class':'datepicker'
                                }),required=False)



    class Meta:
        model = RetryErrors
        fields = '__all__'

    def clean(self):
        cleaned_data = super(RetryErrorsForm, self).clean()
        if cleaned_data['error_diag']:
            try:
                re.compile(cleaned_data['error_diag'])
                is_valid = True
            except re.error:
                is_valid = False
            if not is_valid:
                msg = "Error diag isn't a valid re"
                self._errors['error_diag'] = self.error_class([msg])
                del cleaned_data['error_diag']
        new_work_queue = cleaned_data.get('work_queue')
        if new_work_queue:
            cleaned_data['work_queue'] = new_work_queue.id
        return cleaned_data


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
        fields = '__all__'


class StepExecutionForm(ModelForm):
    class Meta:
        model = StepExecution


class ProductionTaskForm(ModelForm):
    class Meta:
        model = ProductionTask
        fields = '__all__'





class ProductionTaskCreateCloneForm(ModelForm):
    class Meta:
        model = ProductionTask
        fields = '__all__'


def pattern_from_request(reqid):
    """
    Search for a pattern in pattern request. Pattern is the not hided slice with the dataset
    equal to dataset name in 0 slice
    :param reqid: request with pattern
    :return: [slice_number,outputs]
    """
    slices_donor = list(InputRequestList.objects.filter(request=reqid).order_by('slice'))
    container_name = slices_donor[0].dataset.name
    step_for_pattern = StepExecution.objects.filter(slice=slices_donor[0])[0]
    slices_to_get_info = [(int(slices_donor[0].slice),step_for_pattern.step_template.output_formats.split('.'))]
    for index, slice_donor in enumerate(slices_donor[1:]):
        if (slice_donor.dataset.name == container_name) and (not slice_donor.is_hide):
            step_for_pattern = StepExecution.objects.filter(slice=slice_donor)[0]
            slices_to_get_info.append((int(slice_donor.slice),step_for_pattern.step_template.output_formats.split('.')))
    return slices_to_get_info

class ProductionTrainForm(ModelForm):
    manager = CharField(required=True)
    status = CharField(widget=forms.HiddenInput, required=False)
    departure_time = DateTimeField(widget = TextInput(attrs=
                                {
                                    'class':'datepicker'
                                }),required=True)
    description = CharField( widget=Textarea, required=True)
    pattern_request_id = DecimalField(required=True)
    outputs = CharField(widget=forms.HiddenInput,required=False)


    def clean(self):
        cleaned_data = super(ProductionTrainForm, self).clean()
        if cleaned_data.get('pattern_request_id'):
            try:
                cleaned_data['pattern_request'] = TRequest.objects.get(reqid=cleaned_data['pattern_request_id'])
                cleaned_data['outputs'] = json.dumps(pattern_from_request(cleaned_data.get('pattern_request')))
            except Exception,e:
                del cleaned_data['pattern_request_id']
                self._errors['pattern_request_id'] = self.error_class([str(e)])
        return cleaned_data

    class Meta:
        model = TrainProduction
        exclude = ['id','approval_time','timestamp','request','pattern_request']

class ProductionTaskUpdateForm(ModelForm):
    class Meta:
        model = ProductionTask
        widgets = {
            'id': TextInput(attrs={'readonly': 'readonly'}),
        }
        fields = '__all__'