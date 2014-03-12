from django.forms import ModelForm
from django.forms import CharField
from django.forms import EmailField
from django.forms import Textarea
from django.forms import FileField
from django.forms import Form
from models import TRequest, ProductionTask, StepExecution, MCPattern
from django.forms.widgets import TextInput


class RequestForm(ModelForm):
    class Meta:
        model = TRequest


class TRequestCreateCloneConfirmation(ModelForm):
    long_description = CharField(widget=Textarea, required=False)
    cc = EmailField(required=False)

    class Meta:
        model = TRequest
        exclude = ['reqid']


class TRequestMCCreateCloneForm(TRequestCreateCloneConfirmation):
    excellink = CharField(required=False, label="Exel Link")
    excelfile = FileField(required=False, label="Exel File")


class TRequestDPDCreateCloneForm(TRequestCreateCloneConfirmation):
    excellink = CharField(required=False, label="DPD link")
    excelfile = FileField(required=False, label="DPD file")


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