import { BrowserModule } from '@angular/platform-browser';
import {Injectable, LOCALE_ID, NgModule} from '@angular/core';
import { HttpClientModule, HttpClientXsrfModule } from '@angular/common/http';
import {APP_BASE_HREF, DATE_PIPE_DEFAULT_OPTIONS} from '@angular/common';

import { AppComponent } from './app.component';
import { DerivationExclusionComponent } from './derivation-exclusion/derivation-exclusion.component';
import { ProductionRequestComponent } from './production-request/production-request.component';
import {Routes, RouterModule, ExtraOptions, ActivatedRouteSnapshot} from '@angular/router';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { DataCarouselComponent } from './data-carousel/data-carousel.component';
import {MatTableModule} from '@angular/material/table';
import {MatSortModule} from '@angular/material/sort';
import {MatCheckboxModule} from '@angular/material/checkbox';
import {GPDeletionContainerResolver} from './derivation-exclusion/gp-deletion.resolve';
import {MatGridListModule} from '@angular/material/grid-list';
import {DatasetSizePipe} from './derivation-exclusion/dataset-size.pipe';
import {MatButtonModule} from '@angular/material/button';
import {MatSidenavModule} from '@angular/material/sidenav';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatInputModule} from '@angular/material/input';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';
import { RucioURLPipe } from './derivation-exclusion/rucio-url.pipe';
import {MatListModule} from '@angular/material/list';
import { GpStatsComponent } from './derivation-exclusion/gp-stats/gp-stats.component';
import {GPStatsResolver} from './derivation-exclusion/gp-stats/gp-stats.resolve';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import {MatRadioModule} from '@angular/material/radio';
import { GpStatsMatrixComponent } from './derivation-exclusion/gp-stats-matrix/gp-stats-matrix.component';
import { GpContainerInfoComponent } from './derivation-exclusion/gp-container-info/gp-container-info.component';
import { DatasetsTableComponent } from './derivation-exclusion/gp-container-info/datasets-table/datasets-table.component';
import {GpContainerInfoResolver} from './derivation-exclusion/gp-container-info/datasets-table/gp-container-info.resolve';
import { GpApiInstructionComponent } from './derivation-exclusion/gp-api-instruction/gp-api-instruction.component';
import {MatCardModule} from '@angular/material/card';
import {SliceComponent, SliceDetailsDialogComponent} from './production-request/slice/slice.component';
import {NgxChartsModule} from '@swimlane/ngx-charts';
import { StepComponent } from './production-request/step/step.component';
import { ProjectModeComponent } from './production-request/project-mode/project-mode.component';
import {MatTabsModule} from '@angular/material/tabs';
import {ScrollingModule} from '@angular/cdk/scrolling';
import {MatDialogModule} from '@angular/material/dialog';
import { GpDeletionRequestComponent } from './derivation-exclusion/gp-deletion-request/gp-deletion-request.component';
import {MatDatepickerModule} from '@angular/material/datepicker';
import {MAT_DATE_LOCALE, MatNativeDateModule} from '@angular/material/core';
import {MatChipsModule} from '@angular/material/chips';
import {ChipsMultiSelectColoredComponent} from './multi-select-colored-chips/multi-select-colored-chips.component';
import {MatIconModule} from '@angular/material/icon';
import {MatToolbarModule} from '@angular/material/toolbar';
import { TaskStatsComponent } from './production-request/task-stats/task-stats.component';
import {MatSelectModule} from '@angular/material/select';
import {MatExpansionModule} from '@angular/material/expansion';
import { UnmergeCleaningComponent } from './unmerge-cleaning/unmerge-cleaning.component';
import { UnmergeDatasetsComponent } from './unmerge-cleaning/unmerge-datasets/unmerge-datasets.component';
import {MatPaginatorModule} from '@angular/material/paginator';
import {
  SpecialCleaningResolver,
  UnmergeCleaningResolver,
  UnmergeNotDeletedResolver
} from './unmerge-cleaning/unmerge-cleaning.resolver';
import { ProductionTaskComponent } from './production-task/production-task.component';
import {DialogTaskSubmissionComponent, TaskActionComponent} from './task-action/task-action.component';
import {MatMenuModule} from '@angular/material/menu';
import {MatProgressBarModule} from '@angular/material/progress-bar';
import {BPTaskComponent} from "./common/bptask/bptask.component";
import { RequestsliceComponent } from './common/requestslice/requestslice.component';
import { TaskStagingProgressComponent } from './common/task-staging-progress/task-staging-progress.component';
import { TaskProgressComponent } from './common/task-progress/task-progress.component';
import {MatTooltipModule} from "@angular/material/tooltip";
import {
  DerivationFromTagComponent,
  DialogContainerDetails,
  DialogSelectedContainers
} from './derivation-from-tag/derivation-from-tag.component';
import {ClipboardModule} from "@angular/cdk/clipboard";
import {
  BtnCellRenderer,
  DialogTaskDetailsComponent,
  ProductionTaskTableComponent
} from './production-task-table/production-task-table.component';
import { TasksManagementComponent } from './tasks-management/tasks-management.component';
import {AgGridModule} from "ag-grid-angular";
import { TaskDetailsComponent } from './production-request/task-details/task-details.component';
import { TaskTemplateSubmissionComponent } from './group-analysis/task-template-submission/task-template-submission.component';
import {MatStepperModule} from "@angular/material/stepper";
import { PatternEditComponent } from './group-analysis/pattern-edit/pattern-edit.component';
import { AnalysisPatternComponent } from './group-analysis/analysis-pattern/analysis-pattern.component';
import {
  AnalysisRequestComponent,
  DialogRequestOutputsComponent
} from './group-analysis/analysis-request/analysis-request.component';
import { AnalysisSliceComponent } from './group-analysis/analysis-slice/analysis-slice.component';
import {
  AgCellSliceComponent,
  DialogSliceDetailsComponent
} from './group-analysis/ag-cell-slice/ag-cell-slice.component';
import { DynamicFormWFilterComponent } from './dynamic-form-w-filter/dynamic-form-w-filter.component';
import {MatSlideToggleModule} from "@angular/material/slide-toggle";
import { CreateAnalysisRequestComponent } from './group-analysis/create-analysis-request/create-analysis-request.component';
import { DerivationPhysPatternComponent } from './derivation-phys-pattern/derivation-phys-pattern.component';
import { StepTasksComponent } from './group-analysis/step-tasks/step-tasks.component';
import {
  AnalyRequestActionsComponent,
  DialogSliceModificationComponent
} from './group-analysis/analy-request-actions/analy-request-actions.component';
import { AnalysisTemplatesTableComponent } from './group-analysis/analysis-templates-table/analysis-templates-table.component';
import {NgxMatSelectSearchModule} from "ngx-mat-select-search";
import { ParentDrivationForAnalysisComponent } from './group-analysis/parent-drivation-for-analysis/parent-drivation-for-analysis.component';
import { AnalysisApiDescriptionComponent } from './group-analysis/analysis-api-description/analysis-api-description.component';
import { DataCarouselConfigComponent } from './DataCarousel/data-carousel-config/data-carousel-config.component';
import { ChatbotComponent } from './chatbot/chatbot.component';
import { DerivationExtensionComponent } from './derivation-extension/derivation-extension.component';
import {DEFAULTS} from "./common/constants/tasks_constants";
import {DerivationPhysicContainerComponent} from "./derivation-physic-container/derivation-physic-container.component";
import {McSubcampaignsStatComponent} from "./mc-subcampaigns-stat/mc-subcampaigns-stat.component";
// import { BPTaskComponent } from './common/bptask/bptask.component';


const routerOptions: ExtraOptions = {
    scrollPositionRestoration: 'enabled',
    anchorScrolling: 'enabled',
    onSameUrlNavigation: 'reload',
    scrollOffset: [0, 64]
};

@Injectable({ providedIn: 'root' })
export class TaskTitleResolvel {
  resolve(route: ActivatedRouteSnapshot): Promise<string> {
    return Promise.resolve('Task: ' + route.paramMap.get('id'));
  }
}

const routes: Routes = [{path: 'gp-deletion/:data_type/:output', component: DerivationExclusionComponent,
        resolve: {
          gpList: GPDeletionContainerResolver
        } },
      {path: 'gp-stats', component: GpStatsComponent,
        resolve: {
          gpStats: GPStatsResolver,
        },
      },
        {path: 'gp-stats-matrix', component: GpStatsMatrixComponent,
        resolve: {
          gpStats: GPStatsResolver,
        }
        },
        {path: 'gp-container-details/:container', component: GpContainerInfoComponent,
          resolve: {
            gpContainerInfo: GpContainerInfoResolver,
          }
        },
      {path: 'request/ids/:reqIDs', component: ProductionRequestComponent},
  {path: 'request/:jira', component: ProductionRequestComponent},
  {path: 'request-tasks/:id', component: TasksManagementComponent},
  {path: 'request-tasks/:id/:slices', component: TasksManagementComponent},
  {path: 'tasks-by-hashtags/:hashtagString', component: TasksManagementComponent},
  {path: 'tasks-by-dkb/:dkbString', component: TasksManagementComponent},

    {path: 'analysis-template-creation', component: TaskTemplateSubmissionComponent},


    {path: 'analysis-pattern/:tag', component: AnalysisPatternComponent},
      {path: 'create-analysis-request', component: CreateAnalysisRequestComponent},
        {path: 'create-analysis-request/:tag', component: CreateAnalysisRequestComponent},


  {path: 'analysis-templates', component: AnalysisTemplatesTableComponent},

    {path: 'analysis-request/:id', component: AnalysisRequestComponent},

  {path: 'task/:id', component: ProductionTaskComponent, title: TaskTitleResolvel},
  {path: 'gp-deletion-request', component: GpDeletionRequestComponent},
  {path: 'gp-api', component: GpApiInstructionComponent},
    {path: 'ga-api', component: AnalysisApiDescriptionComponent},
  {path: 'input_by_ami_tag/:amiTag', component: DerivationFromTagComponent},
    {path: 'input_by_ami_tag', component: DerivationFromTagComponent},
  {path: 'derivation-phys-pattern', component: DerivationPhysPatternComponent},
  {path: 'dc-config', component: DataCarouselConfigComponent},
  {path: 'chat', component: ChatbotComponent},
  {path: 'derivation-extension/:requestID/:slices', component: DerivationExtensionComponent},

  {path: 'carousel', component: DataCarouselComponent},
  // {path: 'unmerged-deletion/:prefix', component: UnmergeCleaningComponent,
  //   resolve: {
  //     unmergedDatasets: UnmergeCleaningResolver,
  //   }},
    {path: 'unmerged-deletion/notdeleted/:prefix', component: UnmergeCleaningComponent,
    resolve: {
      unmergedDatasets: UnmergeNotDeletedResolver,
    }},
      {path: 'unmerged-deletion/notdeleted/:prefix/:output', component: UnmergeDatasetsComponent,
    resolve: {
      unmergedDatasets: UnmergeNotDeletedResolver,
    }},
    {path: 'derivation-physics-container/:requestID', component: DerivationPhysicContainerComponent},
  {path: 'mc-subcampaign-stats', component: McSubcampaignsStatComponent},

  // {path: 'unmerged-deletion/:prefix/:output', component: UnmergeDatasetsComponent,
  //   resolve: {
  //     unmergedDatasets: UnmergeCleaningResolver,
  //   }},
  {path: 'special-deletion/:prefix/:parentTag/:childTag', component: UnmergeDatasetsComponent,
    resolve: {
      specialDatasets: SpecialCleaningResolver,
    }}];



@NgModule({
  declarations: [
    AppComponent,
    DerivationExclusionComponent,
    ProductionRequestComponent,
    DataCarouselComponent,
    DatasetSizePipe,
    RucioURLPipe,
    GpStatsComponent,
    GpStatsMatrixComponent,
    GpContainerInfoComponent,
    DatasetsTableComponent,
    GpApiInstructionComponent,
    SliceComponent,
    StepComponent,
    ProjectModeComponent,
    SliceDetailsDialogComponent,
    GpDeletionRequestComponent,
    ChipsMultiSelectColoredComponent,
    TaskStatsComponent,
    UnmergeCleaningComponent,
    UnmergeDatasetsComponent,
    ProductionTaskComponent,
    TaskActionComponent,
    DialogTaskSubmissionComponent,
    BPTaskComponent,
    RequestsliceComponent,
    TaskStagingProgressComponent,
    TaskProgressComponent,
    DerivationFromTagComponent,
    DialogContainerDetails,
    DialogSelectedContainers,
    ProductionTaskTableComponent,
    TasksManagementComponent,
    TaskDetailsComponent,
    DialogTaskDetailsComponent,
    BtnCellRenderer,
    TaskTemplateSubmissionComponent,
    PatternEditComponent,
    AnalysisPatternComponent,
    AnalysisRequestComponent,
    AnalysisSliceComponent,
    AgCellSliceComponent,
    DialogSliceDetailsComponent,
    DynamicFormWFilterComponent,
    CreateAnalysisRequestComponent,
    DerivationPhysPatternComponent,
    StepTasksComponent,
    AnalyRequestActionsComponent,
    AnalysisTemplatesTableComponent,
    DialogSliceModificationComponent,
    DialogRequestOutputsComponent,
    ParentDrivationForAnalysisComponent,
    AnalysisApiDescriptionComponent,
    DataCarouselConfigComponent,
    ChatbotComponent,
    DerivationExtensionComponent
  ],
  imports: [
    BrowserModule,
    HttpClientModule,
    HttpClientXsrfModule.withOptions({
      cookieName: 'csrftoken',
      headerName: 'X-CSRFToken',
    }),
    RouterModule.forRoot(routes, routerOptions),
    BrowserAnimationsModule,
    MatTableModule,
    MatSortModule,
    MatCheckboxModule,
    MatGridListModule,
    MatButtonModule,
    MatSidenavModule,
    MatFormFieldModule,
    MatInputModule,
    FormsModule,
    MatListModule,
    MatProgressSpinnerModule,
    MatRadioModule,
    MatCardModule,
    NgxChartsModule,
    MatTabsModule,
    ScrollingModule,
    MatDialogModule,
    MatDatepickerModule,
    ReactiveFormsModule,
    MatNativeDateModule,
    MatChipsModule,
    MatIconModule,
    MatToolbarModule,
    MatSelectModule,
    MatExpansionModule,
    MatPaginatorModule,
    MatMenuModule,
    MatProgressBarModule,
    MatTooltipModule,
    ClipboardModule,
    AgGridModule,
    MatStepperModule,
    MatSlideToggleModule,
    NgxMatSelectSearchModule
  ],
  providers: [
        {
           provide:  APP_BASE_HREF, useValue: '/ng',
          useFactory: getBaseLocation
        },
    {provide: DATE_PIPE_DEFAULT_OPTIONS, useValue: {dateFormat: DEFAULTS.TIME_FORMAT, timezone: 'UTC'}},
      ],
  bootstrap: [AppComponent]
})
export class AppModule { }

function getBaseLocation(): string {
    const paths: string[] = location.pathname.split('/').splice(1, 1);
    const basePath: string = (paths && paths[0]) || '';
    return '/' + basePath;
}

