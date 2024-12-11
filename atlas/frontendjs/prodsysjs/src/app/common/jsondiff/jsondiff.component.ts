import {Component, computed, input, signal} from '@angular/core';
import * as jsondiffpatch from 'jsondiffpatch';
import * as htmlFormatter from 'jsondiffpatch/formatters/html';
import {SafeHTMLPipe} from "../safe-html.pipe";

@Component({
  selector: 'app-jsondiff',
  standalone: true,
  imports: [SafeHTMLPipe],
  template: `<div [innerHTML]="deltaHtml() | safeHTML" style="height: 100%; overflow: auto;"></div>`,
  styleUrl: './jsondiff.component.css'
})
export class JsondiffComponent {
    originalJSON = input({});
    modifiedJSON = input({});
    options = input({});
    deltaHtml = computed(() =>
       htmlFormatter.format(jsondiffpatch.diff(this.originalJSON(), this.modifiedJSON()), this.originalJSON()));

}
