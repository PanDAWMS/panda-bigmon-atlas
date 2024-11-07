import logging
import unittest
from atlas.prodtask.models import TRequest, InputRequestList, TaskTemplate, StepExecution, ProductionTask
import time
from .taskdef import TaskDefinition
from deepdiff import DeepDiff
import json
from .protocol import TaskDefConstants

logger = logging.getLogger('deftcore')

def make_sample_for_slice(request_id, slice_numbers, submit=False):
    slices = InputRequestList.objects.filter(request=request_id, slice__in=slice_numbers)
    approved_steps = []
    for step in StepExecution.objects.filter(request=request_id, status='Approved'):
        if step.slice not in slices and (not ProductionTask.objects.filter(step=step).exists()):
            approved_steps.append(step.id)
            step.status = 'NotChecked'
            step.save()
    try:
        task_def = TaskDefinition()
        if not submit:
            task_def.template_build = 'sample'
            task_def.test_process_requests(request_id, TaskTemplate.TEMPLATE_TYPE.SAMPLE)
        else:
            task_def.force_process_requests([request_id])
    finally:
        for step_id in approved_steps:
            step = StepExecution.objects.get(id=step_id)
            step.status = 'Approved'
            step.save()
        production_request = TRequest.objects.get(reqid=request_id)
        production_request.status = 'test'
        production_request.save()


def get_sample_steps(request_id):
    return [x.step for x in TaskTemplate.objects.filter(request=request_id, template_type=TaskTemplate.TEMPLATE_TYPE.SAMPLE)]


class ParametrizedTestCase(unittest.TestCase):
    """ TestCase classes that want to be parametrized should
        inherit from this class.
    """
    def __init__(self, methodName='runTest', param=None):
        super(ParametrizedTestCase, self).__init__(methodName)
        self.param = param

    @staticmethod
    def parametrize(testcase_class, param=None):
        """ Create a suite containing all tests taken from the given
            subclass, passing them the parameter 'param'.
        """
        test_loader = unittest.TestLoader()
        test_names = test_loader.getTestCaseNames(testcase_class)
        suite = unittest.TestSuite()
        for name in test_names:
            suite.addTest(testcase_class(name, param=param))
        return suite

class VerifyWorkflowInRequest(ParametrizedTestCase):
    @classmethod
    def setUpClass(cls):
        cls._label = 'local_' + time.strftime('%H:%M:%S')

    def setUp(self):
        self._sample_steps = get_sample_steps(self.param)
        logger.info("-"*70)
        logger.info(f"Workflow validation for request: {self.param} number of tests: {len(self._sample_steps)} build: {self._label}")
        td = TaskDefinition()
        td.template_build = self._label
        td.test_process_requests(self.param,  TaskTemplate.TEMPLATE_TYPE.TEST)


    def test_verify_mc(self):
        for step in self._sample_steps:
            with self.subTest(msg=f"{step.request_id} - {step.slice.slice} - {step.slice.comment}"):
                test_task = TaskTemplate.objects.get(step=step, template_type=TaskTemplate.TEMPLATE_TYPE.TEST, request=self.param,
                                                     build=self._label)
                sample_task = TaskTemplate.objects.get(step=step, template_type=TaskTemplate.TEMPLATE_TYPE.SAMPLE, request=self.param)
                self.assertEqual(str(DeepDiff(json.loads(sample_task.task_template), json.loads(test_task.task_template), ignore_order=True, view='tree')), '{}')



def run_test_interactive():
    suite = unittest.TestSuite()
    for request_id in TaskDefConstants.WORKFLOW_VALIDATION_REQUESTS:
        suite.addTest(ParametrizedTestCase.parametrize(VerifyWorkflowInRequest, param=request_id))
    runner = unittest.TextTestRunner()
    runner.run(suite)