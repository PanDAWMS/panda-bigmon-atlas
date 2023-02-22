import {Component, OnInit} from '@angular/core';

import {ActivatedRoute} from '@angular/router';


@Component({
  selector: 'app-production-task',
  templateUrl: './production-task.component.html',
  styleUrls: ['./production-task.component.css']
})
export class ProductionTaskComponent implements OnInit{




  constructor(public route: ActivatedRoute) {
  }

  ngOnInit(): void  {

  }


  parseID(id: string): number {
    return Number(id);
  }
}
