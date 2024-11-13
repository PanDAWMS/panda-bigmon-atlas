import logging
import unittest
from typing import Optional

from atlas.prodtask.models import TRequest, InputRequestList, TaskTemplate, StepExecution, ProductionTask, TTask
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

def compare_slices(source_request_id: int, source_slices: [int], target_slices: [int], target_request_id: Optional[int] = None):

    def get_tasks_slices(request_id, slices) -> [ProductionTask]:
        steps = StepExecution.objects.filter(request=request_id, slice__in=InputRequestList.objects.filter(request=request_id, slice__in=slices))
        return ProductionTask.objects.filter(step__in=steps, request=request_id).order_by('id')

    if not target_request_id:
        target_request_id = source_request_id
    source_tasks: [ProductionTask] = get_tasks_slices(source_request_id, source_slices)
    target_tasks: [ProductionTask] = get_tasks_slices(target_request_id, target_slices)

    assert source_tasks.count() == target_tasks.count(), f"Number of tasks is different {source_tasks.count()} != {target_tasks.count()}"
    for source_task, target_task in zip(source_tasks, target_tasks):
        print(f"Comparing tasks {source_task.id} == {target_task.id}")
        source_ttask_json_str = TTask.objects.get(id=source_task.id)._jedi_task_parameters
        target_ttask_json_str = TTask.objects.get(id=target_task.id)._jedi_task_parameters
        source_ttask_json_str = (source_ttask_json_str.replace(str(source_task.id),'task_id').replace(str(source_task.project),'project').
                                 replace(str(source_task.parent_id),'parent_id'))
        target_ttask_json_str = (target_ttask_json_str.replace(str(target_task.id),'task_id').replace(str(target_task.project),'project').
                                replace(str(target_task.parent_id),'parent_id'))
        deep_diff = DeepDiff(json.loads(source_ttask_json_str), json.loads(target_ttask_json_str),
                             exclude_paths=['ttcrTimestamp'], ignore_order=True, view='tree', exclude_regex_paths=r"root\['jobParameters'\]\[\d+\]\['dataset'\]")
        if deep_diff:
            print(deep_diff)
        assert str(DeepDiff(json.loads(source_ttask_json_str), json.loads(target_ttask_json_str), exclude_regex_paths=r"root\['jobParameters'\]\[\d+\]\['dataset'\]",
                            exclude_paths=['ttcrTimestamp'], ignore_order=True, view='tree')) == '{}', f"Tasks are different {source_task.id} != {target_task.id}"