import {Component, computed, inject, input, OnInit, signal} from '@angular/core';
import {FormBuilder, ReactiveFormsModule, Validators} from "@angular/forms";
import {MatFormField, MatInput, MatLabel} from "@angular/material/input";
import {MatOption} from "@angular/material/core";
import {MatSelect} from "@angular/material/select";
import {DATASETS_NOMENCLATURE} from "../../common/constants/tasks_constants";
import {EventPickingService} from "./event-picking.service";
import {setErrorMessage} from "../../dsid-info/dsid-info.service";
import {Router} from "@angular/router";

@Component({
  selector: 'app-event-picking-request-creation',
  imports: [
    ReactiveFormsModule,
    MatInput,
    MatFormField,
    MatLabel,
    MatOption,
    MatSelect
  ],
  templateUrl: './event-picking-request-creation.component.html',
  styleUrl: './event-picking-request-creation.component.css'
})
export class EventPickingRequestCreationComponent implements OnInit {
  protected readonly DATASETS_NOMENCLATURE = DATASETS_NOMENCLATURE;
  readonly submitting = signal(false);
  inputFileElement: null|HTMLInputElement = null;
  jira = input('');
  // Keep file outside the form; Angular forms don't serialize File cleanly.
  readonly inputFile = signal<File | null>(null);
  eventPickingService = inject(EventPickingService);
  fileContent = signal<string | null>(null);
  runsAndEvents = computed(() => {
    if (this.fileContent()){
    //   each string has two numbers separated by space; return number of unique objects in the first column and total number of objects
      const uniqueRuns = new Set<string>();
      let totalEvents = 0;
      const lines = this.fileContent().split('\n');
      for (const line of lines) {
        const parts = line.trim().split(' ');
        if (parts.length === 2 && !isNaN(Number(parts[0])) && !isNaN(Number(parts[1]))) {
          uniqueRuns.add(parts[0]);
          totalEvents += 1;
        }
      }
      return {runs: uniqueRuns.size, events: totalEvents};
    }
  });
  inputFileText = computed(() => this.fileContent() ?  'Upload another' : 'Upload file' );
  errorMessage: string | null = null;
  readonly form = this.fb.nonNullable.group({
    JIRA: this.fb.control<string | null>('', [Validators.required, Validators.maxLength(200)]),
    stream: this.fb.control<string | null>(null, [ Validators.maxLength(64)]),
    description: this.fb.control<string | null>(null, [ Validators.required, Validators.maxLength(2000)]),
    data_format: this.fb.control<string | null>('RAW', [Validators.maxLength(20)]),
    merge: this.fb.control<boolean>(false),
    submit: this.fb.control<boolean>(false)

  });

  readonly canSubmit = computed(() => this.fileContent() && !this.submitting());


  validateFileContent(content: string): boolean {
    // check that file contains line with exactly two numbers separated by space
    const lines = content.split('\n');
    if (lines.length === 0) {
      return false;
    }
    for (const line of lines) {
      if (line.trim() !== ''){
        const parts = line.trim().split(' ');
        if (!(parts.length === 2 && parts.every(part => !isNaN(Number(part))))) {
          return false;
        }
      }
    }
    return true;
  }

  ngOnInit(): void{
    this.form.get('JIRA').setValue(this.jira());
    if (this.jira()) {
      this.eventPickingService.getEPRequestForUpdate(this.jira()).subscribe({
        next: response => {
          if (response) {
            this.form.get('description').setValue(response.description);
            this.form.get('merge').setValue(response.merge);
          }
        }
      });
    }
  }

  cleanFileContent(): void {
    this.fileContent.set(null);
    this.inputFile.set(null);
    if (this.inputFileElement){
      this.inputFileElement.value = '';
    }
  }
  onInputFileChange(e: Event): void {
    this.inputFileElement = e.target as HTMLInputElement;
    const file = this.inputFileElement.files?.item(0) ?? null;
    this.inputFile.set(file);
    this.inputFile().text().then(
      content => {
        if (this.validateFileContent(content)) {
          const newContent = this.fileContent() ? this.fileContent() + '\n' + content : content;
          this.fileContent.set(newContent);
          this.errorMessage = null;
        } else {
          this.errorMessage = 'Invalid file format. Each line must contain exactly two numbers separated by space.';
        }
      }
    ).catch(
      error => this.errorMessage = `Failed to read file: ${error}`
    );
  }
  constructor(private readonly fb: FormBuilder, private router: Router) {}

    submit(): void {
      this.submitting.set(true);
      if (!this.canSubmit()) {
        this.eventPickingService.createOrUpdateEPRequest(
          this.form.get('JIRA')?.value ?? '',
          this.form.get('stream')?.value ?? '',
          this.form.get('description')?.value ?? '',
          this.form.get('data_format')?.value ?? '',
          this.form.get('merge').value,
          this.form.get('submit').value,
          this.fileContent() ?? ''
        ).subscribe({
          next: response => {
            this.eventPickingService.EPProgressResource.reload();
            this.router.navigate(['/event-picking-request', response]);
          },
          error: err => {
            this.errorMessage = setErrorMessage(err);
            this.submitting.set(false);
          }
        }
        );
      }
    }


}
