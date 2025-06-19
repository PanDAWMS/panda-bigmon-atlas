import {Component, input} from '@angular/core';
import {RucioDIDComponent} from "../production-request/rucio-did/rucio-did.component";

@Component({
  selector: 'app-datatset-details',
  imports: [
    RucioDIDComponent
  ],
  templateUrl: './datatset-details.component.html',
  styleUrl: './datatset-details.component.css'
})
export class DatatsetDetailsComponent {
    did= input<string>('');
}
