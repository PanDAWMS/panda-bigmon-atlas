import {Component, effect, input, model, OnInit, viewChild, ViewContainerRef} from '@angular/core';
import JSONEditor, {JSONEditorOptions} from 'jsoneditor';

@Component({
  selector: 'app-prodsys-jsoneditor',
  standalone: true,
  imports: [],
  template: `<div #editor style="height: 100%"></div>`,
  styleUrl: './prodsys-jsoneditor.component.css'
})
export class ProdsysJsoneditorComponent implements OnInit {
    baseContainer = viewChild('editor', {read: ViewContainerRef});
    editorOptions = input({});
    schema = input({});
    modelJson = model<any>({});
    errorState = model(false);
    private editor: JSONEditor;

    ngOnInit(): void {
        const currentOptions = this.prepareEditorOptions(this.editorOptions());
        this.editor = new JSONEditor(this.baseContainer()?.element.nativeElement, currentOptions, this.modelJson());

    }
  constructor() {
      effect(() => {
          if (this.editor && (JSON.stringify(this.modelJson()) !== JSON.stringify(this.editor.get()))) {
            this.editor.set(this.modelJson());
            this.editor.setSchema(this.schema());
          }
        });
  }
    public prepareEditorOptions(options: JSONEditorOptions | undefined): JSONEditorOptions{
      if (!options) {
        options = {};
      }
      options.onChangeJSON = this.onChangeJSON.bind(this);
      options.onChange = this.onChangeJSON.bind(this);
      options.onValidationError = this.onValidationError.bind(this);
      options.schema = this.schema();
      return options;
    }
    public onChangeJSON(): void {
      let newJson = this.modelJson();
      try {
        newJson = this.editor.get();
        this.errorState.set(false);
      } catch (e) {
        this.errorState.set(true);
      }
      if (JSON.stringify(newJson) !== JSON.stringify(this.modelJson())) {
        this.modelJson.set(this.editor.get());
      }
    }

    public onValidationError(error: any): void {
      if (error && error.length > 0) {
        this.errorState.set(true);
      }
    }
}
