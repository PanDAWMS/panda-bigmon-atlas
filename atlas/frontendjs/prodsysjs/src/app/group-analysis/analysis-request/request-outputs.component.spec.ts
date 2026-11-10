import {TestBed} from '@angular/core/testing';
import {MAT_DIALOG_DATA, MatDialogRef} from '@angular/material/dialog';
import {of, Subject} from 'rxjs';
import {AnalysisTasksService} from '../analysis-tasks.service';
import {DialogRequestOutputsComponent} from './analysis-request.component';

describe('Analysis request output container creation', () => {
  let component: DialogRequestOutputsComponent;
  let service: jasmine.SpyObj<AnalysisTasksService>;

  beforeEach(() => {
    service = jasmine.createSpyObj('AnalysisTasksService', [
      'getAnalysisContainerScopes', 'getAnalysisRequestOutputs', 'registerAnalysisOutputContainer'
    ]);
    service.getAnalysisContainerScopes.and.returnValue(of(['user.analyst', 'group.phys-test']));
    service.getAnalysisRequestOutputs.and.returnValue(of(['dataset.one', 'dataset.two']));
    TestBed.configureTestingModule({providers: [
      {provide: MAT_DIALOG_DATA, useValue: {requestID: '1234'}},
      {provide: MatDialogRef, useValue: {}},
      {provide: AnalysisTasksService, useValue: service}
    ]});
    component = TestBed.runInInjectionContext(() => new DialogRequestOutputsComponent());
    component.ngOnInit();
  });

  it('keeps loading until datasets arrive, independently of scopes', () => {
    const response = new Subject<string[]>();
    service.getAnalysisRequestOutputs.and.returnValue(response);
    const pendingDialog = TestBed.runInInjectionContext(() => new DialogRequestOutputsComponent());
    pendingDialog.ngOnInit();
    expect(service.getAnalysisRequestOutputs).toHaveBeenCalledWith('1234');
    expect(pendingDialog.loadingOutputs).toBeTrue();
    expect(pendingDialog.loadingScopes).toBeFalse();
    expect(pendingDialog.outputs).toEqual([]);
    response.next(['dataset.one', 'dataset.one', 'dataset.two']);
    response.complete();
    expect(pendingDialog.loadingOutputs).toBeFalse();
    expect(pendingDialog.outputs).toEqual(['dataset.one', 'dataset.two']);
    expect(pendingDialog.selectedOutputs.size).toBe(2);
  });

  it('stops loading and shows dataset errors', () => {
    const response = new Subject<string[]>();
    service.getAnalysisRequestOutputs.and.returnValue(response);
    const pendingDialog = TestBed.runInInjectionContext(() => new DialogRequestOutputsComponent());
    pendingDialog.ngOnInit();
    response.error({error: 'Unable to load datasets'});
    expect(pendingDialog.loadingOutputs).toBeFalse();
    expect(pendingDialog.outputsErrorMessage).toBe('Unable to load datasets');
    expect(pendingDialog.selectedOutputs.size).toBe(0);
  });

  it('defaults to the user scope and leaves the container name empty', () => {
    expect(component.selectedScope.value).toBe('user.analyst');
    expect(component.userScopes).toEqual(['user.analyst', 'group.phys-test']);
    expect(component.containerName.value).toBe('');
    component.createContainer();
    expect(service.registerAnalysisOutputContainer).not.toHaveBeenCalled();
  });

  it('creates a container from the selected datasets using the chosen group scope', () => {
    service.registerAnalysisOutputContainer.and.returnValue(of('Container registered'));
    component.selectedScope.setValue('group.phys-test');
    component.containerName.setValue('results');
    component.selectOutput('dataset.two', false);
    component.createContainer();
    expect(service.registerAnalysisOutputContainer).toHaveBeenCalledWith(
      '1234', 'group.phys-test', 'results', ['dataset.one']);
    expect(component.createdContainers).toEqual(['group.phys-test.results']);
    expect(component.submitStatus).toBe('Container registered');
    expect(component.creatingContainer).toBeFalse();
  });

  it('requires a selection', () => {
    component.containerName.setValue('results');
    component.selectAll(false);
    component.createContainer();
    expect(service.registerAnalysisOutputContainer).not.toHaveBeenCalled();
    component.selectAll(true);
    expect(component.selectedOutputs.size).toBe(2);
  });

  it('only submits selected datasets matching the filter, even if hidden datasets remain selected', () => {
    service.registerAnalysisOutputContainer.and.returnValue(of('Container registered'));
    component.containerName.setValue('results');
    component.outputFilter.setValue(' ONE ');
    expect(component.filteredOutputs).toEqual(['dataset.one']);
    expect(component.selectedOutputs.size).toBe(2);
    component.createContainer();
    expect(service.registerAnalysisOutputContainer).toHaveBeenCalledWith(
      '1234', 'user.analyst', 'results', ['dataset.one']);
  });

  it('does not submit when the filter matches no datasets', () => {
    component.containerName.setValue('results');
    component.outputFilter.setValue('missing');
    component.createContainer();
    expect(component.selectedFilteredOutputs).toEqual([]);
    expect(service.registerAnalysisOutputContainer).not.toHaveBeenCalled();
  });

  it('selects and deselects only filtered datasets, preserving individual selections', () => {
    component.outputFilter.setValue('one');
    component.selectAll(false);
    expect(component.selectedFilteredOutputs).toEqual([]);
    expect(component.selectedOutputs.has('dataset.two')).toBeTrue();
    component.selectAll(true);
    expect(component.selectedFilteredOutputs).toEqual(['dataset.one']);
    component.selectOutput('dataset.one', false);
    expect(component.selectedFilteredOutputs).toEqual([]);
    component.outputFilter.setValue('');
    expect(component.selectedFilteredOutputs).toEqual(['dataset.two']);
  });

  it('prevents duplicate submissions and shows registration errors', () => {
    const response = new Subject<string>();
    service.registerAnalysisOutputContainer.and.returnValue(response);
    component.containerName.setValue('results');
    component.createContainer();
    component.createContainer();
    expect(service.registerAnalysisOutputContainer).toHaveBeenCalledTimes(1);
    response.error({error: 'Rucio unavailable'});
    expect(component.errorMessage).toBe('Rucio unavailable');
    expect(component.createdContainers).toEqual([]);
    expect(component.creatingContainer).toBeFalse();
  });
});
