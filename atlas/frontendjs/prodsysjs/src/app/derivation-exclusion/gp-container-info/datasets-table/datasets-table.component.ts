import {Component, Input} from '@angular/core';
import {Dataset} from "../gp-container-info.service";

@Component({
    selector: 'app-datasets-table',
    templateUrl: './datasets-table.component.html',
    styleUrls: ['./datasets-table.component.css'],
    standalone: false
})
export class DatasetsTableComponent{

  @Input() datasetsDataSource: Dataset[];

}
