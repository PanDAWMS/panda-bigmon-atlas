import { Component } from '@angular/core';
import {
  DerivationPhysicContainerService,
  PhysicsContainer,
  PhysicsContainerIndex
} from "./derivation-physic-container.service";
import {ActivatedRoute} from "@angular/router";
import {catchError, map, switchMap, tap} from "rxjs/operators";
import {AsyncPipe, JsonPipe} from "@angular/common";
import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";
import {BehaviorSubject, combineLatest, merge, Observable, of, Subject} from "rxjs";
import {FormsModule} from "@angular/forms";
import {MatSlideToggleModule} from "@angular/material/slide-toggle";

@Component({
  selector: 'app-derivation-physic-container',
  standalone: true,
  imports: [
    AsyncPipe,
    JsonPipe,
    MatProgressSpinnerModule,
    FormsModule,
    MatSlideToggleModule
  ],
  templateUrl: './derivation-physic-container.component.html',
  styleUrl: './derivation-physic-container.component.css'
})
export class DerivationPhysicContainerComponent {

  constructor(private derivationPhysicContainerService: DerivationPhysicContainerService, private route: ActivatedRoute) { }
  error: string | undefined;
  grlPath = '';
  notFullExists = false;
  allowNotFull = false;
  missingContainers = false;
  loading = false;
  createState: 'not_send'| 'sent'| 'error'| 'created' = 'not_send';
  currentContainers: PhysicsContainer[];
  requestID: number| undefined;
  createMessage: string;
  updatePhysicContainer$ = new BehaviorSubject<string>('');
  public preparedContainers$: Observable<PhysicsContainerIndex|undefined> = combineLatest([this.updatePhysicContainer$, this.route.paramMap]).pipe(
    map( ([grl, params]) => {
      this.loading = true;
      this.requestID = Number(params.get('requestID'));
      return this.derivationPhysicContainerService.getPhysicContainers(params.get('requestID'), grl );
  }
  )).pipe(switchMap( result => result), tap(x => {
        this.loading = false;
        this.currentContainers = x.containers;
        this.grlPath = x.grl;
        this.notFullExists = false;
        this.missingContainers = false;
        for (const container of x.containers) {
          if (container.not_full_containers.length > 0) {
            this.notFullExists = true;
            break;
          }
        }
        for (const container of x.containers) {
          if (container.missing_containers.length > 0) {
            this.missingContainers = true;
            break;
          }
        }
      }
    ),
    catchError((err) => {
      this.loading = false;
      this.error = err?.error;
      if (err?.error !== undefined) {
        this.error = err.error;
      } else if (err?.message !== undefined) {
        this.error = err.message;
      } else {
        this.error = err.toString();
      }
      return [];
  }));

  useGrl(): void {
    this.updatePhysicContainer$.next(this.grlPath);
  }

  createPhysicsContainers(): void {
    this.createState = 'sent';
    this.derivationPhysicContainerService.createPhysicsContainer(this.currentContainers).pipe(
      catchError((err) => {
        this.createState = 'error';
        this.error = err?.error;
        if (err?.error !== undefined) {
          this.error = err.error;
        } else if (err?.message !== undefined) {
          this.error = err.message;
        } else {
          this.error = err.toString();
        }
        return of([]);
      }
    )).subscribe( result => {
        if (result.length > 0) {
          this.createState = 'created';
        }
    });
  }
}
