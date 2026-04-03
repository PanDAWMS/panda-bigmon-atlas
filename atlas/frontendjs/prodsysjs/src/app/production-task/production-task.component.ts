import { Component, OnInit, inject } from '@angular/core';

import {ActivatedRoute} from '@angular/router';


@Component({
    selector: 'app-production-task',
    templateUrl: './production-task.component.html',
    styleUrls: ['./production-task.component.css'],
    standalone: false
})
export class ProductionTaskComponent implements OnInit{
  route = inject(ActivatedRoute);


  ngOnInit(): void  {

  }


  parseID(id: string): number {
    return Number(id);
  }
}
