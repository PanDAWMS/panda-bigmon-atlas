import { Component } from '@angular/core';
import { HttpClient } from '@angular/common/http';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})

export class AppComponent {
  title = 'app';
  url = '/dkb/test_name';
  myName = '';
  baseAPI = '';

  constructor(private http: HttpClient) {}
  public getName(): void {
       this.http.get<string>(this.baseAPI + this.url).subscribe(name => this.myName = name);
  }


}
