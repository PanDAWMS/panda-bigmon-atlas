from django import forms
from django.forms import ModelForm
from django.forms import CharField
#from django.forms import EmailField
#from django.forms import Textarea
#from django.forms import FileField
#from django.forms import DecimalField
#from django.forms import Form
from models import TRequest
#from django.forms.widgets import TextInput


class RequestForm(ModelForm):
	request =  CharField(label='Request', required=False)

	class Meta:
		model = TRequest
