import {createEntityAdapter, EntityState} from "@ngrx/entity";
import {AnalysisSlice} from "../analysis-task-model";
import {Component, Injectable} from "@angular/core";
import {ComponentStore} from "@ngrx/component-store";

interface AnalysisRequestState extends EntityState<AnalysisSlice> {
  loading: boolean;
  error?: string;
}

const analysisRequestAdapter = createEntityAdapter<AnalysisSlice>();

const initialState: AnalysisRequestState = analysisRequestAdapter.getInitialState({
  loading: false,
  error: null
});

@Injectable()
export class AnalysisRequestStore extends ComponentStore<AnalysisRequestState> {
  constructor() {
    super(initialState);
  }

  readonly loading$ = this.select(state => state.loading);
  readonly error$ = this.select(state => state.error);
  readonly slices$ = this.select(state => analysisRequestAdapter.getSelectors().selectAll(state));

  readonly setLoading = this.updater((state, loading: boolean) => ({
    ...state,
    loading
  }));
  }
}
