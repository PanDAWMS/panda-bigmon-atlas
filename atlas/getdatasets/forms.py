from django import forms
from django.forms import ModelForm
from django.forms import CharField
from models import TRequest

class RequestForm(ModelForm):
	request =  CharField(label='Request', required=False)

	class Meta:
		model = TRequest
