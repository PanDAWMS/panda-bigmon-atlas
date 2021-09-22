import {Component, Input, OnInit} from '@angular/core';

@Component({
  selector: 'app-project-mode',
  templateUrl: './project-mode.component.html',
  styleUrls: ['./project-mode.component.css']
})
export class ProjectModeComponent implements OnInit {
  @Input() projectMode: string;

  constructor() { }

  ngOnInit(): void {
  }

}
