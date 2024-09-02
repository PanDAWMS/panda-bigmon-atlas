import {Component, Input} from '@angular/core';

@Component({
  selector: 'app-bptask',
  template: `<a href="https://bigpanda.cern.ch/task/{{task}}/">{{ task }}</a>`,
  standalone: true,
  styleUrls: ['./bptask.component.css']
})
export class BPTaskComponent  {
  @Input() task: string|number;
}
