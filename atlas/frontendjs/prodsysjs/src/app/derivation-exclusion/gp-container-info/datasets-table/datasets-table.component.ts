import {Component, Input, OnInit} from '@angular/core';
import {MatTableDataSource} from "@angular/material/table";
import {Dataset} from "../gp-container-info.service";

@Component({
  selector: 'app-datasets-table',
  templateUrl: './datasets-table.component.html',
  styleUrls: ['./datasets-table.component.css']
})
export class DatasetsTableComponent{

  @Input() datasetsDataSource: Dataset[];

}
