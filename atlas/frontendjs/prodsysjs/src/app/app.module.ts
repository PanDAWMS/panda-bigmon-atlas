import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { HttpClientModule, HttpClientXsrfModule } from '@angular/common/http';
import { APP_BASE_HREF } from '@angular/common';

import { AppComponent } from './app.component';
import { DerivationExclusionComponent } from './derivation-exclusion/derivation-exclusion.component';
import { ProductionRequestComponent } from './production-request/production-request.component';
import {Routes, RouterModule, ExtraOptions} from '@angular/router';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { DataCarouselComponent } from './data-carousel/data-carousel.component';
import {MatTableModule} from '@angular/material/table';
import {MatSortModule} from '@angular/material/sort';
import {MatCheckboxModule} from '@angular/material/checkbox';
import {GPDeletionContainerResolver} from './derivation-exclusion/gp-deletion.resolve';
import {MatGridListModule} from "@angular/material/grid-list";
import {DatasetSizePipe} from "./derivation-exclusion/dataset-size.pipe";
import {MatButtonModule} from "@angular/material/button";
import {MatSidenavModule} from "@angular/material/sidenav";
import {MatFormFieldModule} from "@angular/material/form-field";
import {MatInputModule} from "@angular/material/input";
import {FormsModule} from "@angular/forms";
import { RucioURLPipe } from './derivation-exclusion/rucio-url.pipe';
import {MatListModule} from "@angular/material/list";
import { GpStatsComponent } from './derivation-exclusion/gp-stats/gp-stats.component';
import {GPStatsResolver} from './derivation-exclusion/gp-stats/gp-stats.resolve';
import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";
import {MatRadioModule} from "@angular/material/radio";
import { GpStatsMatrixComponent } from './derivation-exclusion/gp-stats-matrix/gp-stats-matrix.component';
import { GpContainerInfoComponent } from './derivation-exclusion/gp-container-info/gp-container-info.component';
import { DatasetsTableComponent } from './derivation-exclusion/gp-container-info/datasets-table/datasets-table.component';
import {GpContainerInfoResolver} from "./derivation-exclusion/gp-container-info/datasets-table/gp-container-info.resolve";
import { GpApiInstructionComponent } from './derivation-exclusion/gp-api-instruction/gp-api-instruction.component';
import {MatCardModule} from "@angular/material/card";
import { SliceComponent } from './production-request/slice/slice.component';


const routerOptions: ExtraOptions = {
    scrollPositionRestoration: 'enabled',
    anchorScrolling: 'enabled',
    onSameUrlNavigation: 'reload',
    scrollOffset: [0, 64],
    relativeLinkResolution: 'legacy'
}{
    scrollPositionRestoration: 'enabled',
    anchorScrolling: 'enabled',
    onSameUrlNavigation: 'reload',
    scrollOffset: [0, 64],
    relativeLinkResolution: 'legacy'
};

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
      {path: 'request', component: ProductionRequestComponent},
  {path: 'gp-api', component: GpApiInstructionComponent},

  {path: 'carousel', component: DataCarouselComponent}];

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


  ],
  providers: [
        {
          provide: APP_BASE_HREF,
          useFactory: getBaseLocation
        }
      ],
  bootstrap: [AppComponent]
})
export class AppModule { }

function getBaseLocation(): string {
    const paths: string[] = location.pathname.split('/').splice(1, 1);
    const basePath: string = (paths && paths[0]) || '';
    return '/' + basePath;
}

