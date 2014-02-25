from django.forms import ModelForm
from django.forms import CharField
from django.forms import EmailField
from django.forms import Textarea
from django.forms import FileField
from models import TRequest, ProductionTask, StepExecution
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

class TRequestCreateCloneForm(TRequestCreateCloneConfirmation):
    excellink = CharField(required=False)
    excelfile = FileField(required=False)  

class RequestUpdateForm(ModelForm):
    class Meta:
        model = TRequest
        widgets = {
            'reqid': TextInput(attrs={'readonly':'readonly'}),
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
            'id': TextInput(attrs={'readonly':'readonly'}),
        }